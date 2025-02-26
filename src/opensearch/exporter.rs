use std::error::Error;

use opensearch::{BulkParts, OpenSearch, http::request::JsonBody};
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    common::v1::any_value::Value as OtelValue,
};

use crate::{core::error::ApplicationError, utils::chrono::format_iso8601};

use super::mapper::map_otel_value_to_serdejson_value;

#[derive(Debug)]
pub struct Exporter {
    client: OpenSearch,
    index: String,
    index_append_date_suffix: bool,

    processed_log_record: Counter<u64>,
    bulk_record_length: opentelemetry::metrics::Histogram<u64>,
    bulk_request_duration: opentelemetry::metrics::Histogram<u64>,
    bulk_request_record_error: Counter<u64>,
    retryable_error: Counter<u64>,
    non_retryable_error: Counter<u64>,
}

impl Exporter {
    pub fn new(
        client: OpenSearch,
        index: String,
        index_append_date_suffix: bool,
    ) -> Self {
        let meter = opentelemetry::global::meter("exporter");
        let processed_log_record = meter
            .u64_counter("processed_log_record")
            .build();
        let bulk_record_length = meter
            .u64_histogram("bulk_record_length")
            .with_boundaries([1.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 10000.0].to_vec())
            .build();
        let bulk_request_duration = meter
            .u64_histogram("bulk_request_duration")
            .with_unit("ms")
            .build();
        let bulk_request_record_error = meter
            .u64_counter("bulk_request_record_error")
            .build();
        let retryable_error = meter
            .u64_counter("retryable_error")
            .build();
        let non_retryable_error = meter
            .u64_counter("non_retryable_error")
            .build();

        Self {
            client,
            index,
            index_append_date_suffix,
            processed_log_record,
            bulk_record_length,
            bulk_request_duration,
            bulk_request_record_error,
            retryable_error,
            non_retryable_error,
        }
    }
}

impl Exporter {
    pub async fn export(
        &self,
        request: ExportLogsServiceRequest,
    ) -> Result<ExportLogsServiceResponse, ApplicationError> {
        let bulk_create_template = serde_json::json!({"create": {}});
        let mut bulk_body: Vec<JsonBody<serde_json::Value>> = Vec::new();
        for resouce_log in request.resource_logs {
            for scope_log in resouce_log.scope_logs {
                for log_record in scope_log.log_records {
                    let mut log = serde_json::Map::new();
                    let mut attributes = serde_json::Map::new();
                    for attribute in log_record.attributes {
                        if let Some(v) = attribute.value {
                            attributes
                                .insert(attribute.key, map_otel_value_to_serdejson_value(v.value));
                        }
                    }
                    log.insert(
                        "attributes".to_owned(),
                        serde_json::Value::Object(attributes),
                    );
                    
                    if let Some(b) = log_record.body {
                        if let Some(v) = b.value {
                            let b = match v {
                                OtelValue::StringValue(s) => {
                                    if let Ok(parsed) = serde_json::from_str(&s) {
                                        parsed
                                    } else {
                                        serde_json::Value::String(s)
                                    }
                                }
                                OtelValue::KvlistValue(kv) =>
                                    map_otel_value_to_serdejson_value(Some(OtelValue::KvlistValue(kv))),
                                vv => {
                                    // TODO: we should add metric to track how often we hit this case.
                                    serde_json::Value::String(
                                        map_otel_value_to_serdejson_value(Some(vv))
                                            .to_string()
                                    )
                                }
                            };
                            // IMPORTANT: b must be an object or a string.
                            if b.is_object() {
                                log.insert("body".to_owned(), b);
                            } else {
                                log.insert("raw_body".to_owned(), b);
                            }
                        }
                    }
                    log.insert(
                        "@timestamp".to_owned(),
                        serde_json::Value::String(
                            format_iso8601(chrono::DateTime::from_timestamp_nanos(log_record.time_unix_nano as i64))
                        ),
                    );
                    bulk_body.push(bulk_create_template.clone().into());
                    bulk_body.push(Into::<serde_json::Value>::into(log).into());
                    self.processed_log_record.add(1, &[]);
                }
            }
        }

        let index = if self.index_append_date_suffix {
            &format!("{}-{}", self.index, chrono::Local::now().format("%y%m%d"))
        } else {
            &self.index
        };

        let bulk_document_length = (bulk_body.len() as u64) / 2;
        self.bulk_record_length.record(bulk_document_length, &[]);
        let start = std::time::Instant::now();
        let bulk_response = self
            .client
            .bulk(BulkParts::Index(index))
            .body(bulk_body)
            .send()
            .await;
        let elapsed = start.elapsed();
        self.bulk_request_duration.record(elapsed.as_millis() as u64, &[]);

        match bulk_response {
            Ok(resp) => {
                let resp_body: serde_json::Value = match resp.json().await {
                    Ok(resp_body) => resp_body,
                    Err(err) => {
                        // TODO: logging
                        self.non_retryable_error.add(1, &[]);
                        return Err(ApplicationError::NonRetryable(err.into()));
                    }
                };

                match resp_body["errors"].as_bool() {
                    Some(true) => {
                        let Some(items) = resp_body["items"].as_array() else {
                            // TODO: logging
                            self.non_retryable_error.add(1, &[]);
                            return Err(ApplicationError::NonRetryable(
                                "cannot find items in bulk error respose".into(),
                            ));
                        };

                        let mut error_count = 0;
                        for it in items {
                            let index = match it["index"].as_object() {
                                Some(v) => v,
                                None => {
                                    // TODO: logging
                                    error_count += 1;
                                    continue;
                                }
                            };
                            let status = match index["status"].as_i64() {
                                Some(v) => v,
                                None => {
                                    // TODO: logging
                                    error_count += 1;
                                    continue;
                                }
                            };

                            // TODO: do we need to check for 201.
                            if status != 200 || status != 201 {
                                error_count += 1;
                            }
                        }

                        self.bulk_request_record_error.add(error_count as u64, &[]);
                        return Ok(ExportLogsServiceResponse {
                            partial_success: Some(ExportLogsPartialSuccess {
                                rejected_log_records: error_count,
                                // TODO: optimize, maybe we should return only _id and reason for each error.
                                error_message: resp_body.to_string(),
                            }),
                        });
                    }
                    Some(false) => {
                        return Ok(ExportLogsServiceResponse {
                            ..Default::default()
                        });
                    }
                    None => {
                        // TODO: logging
                        return Ok(ExportLogsServiceResponse {
                            ..Default::default()
                        });
                    }
                }
            }
            Err(err) => {
                if let Some(err) = err.source() {
                    if let Some(ioerr) = err.downcast_ref::<std::io::Error>() {
                        self.retryable_error.add(1, &[]);
                        // IMPORTANT: we should retry only on IO error
                        return Err(ApplicationError::Retryable(ioerr.to_string().into()));
                    }
                }
                self.non_retryable_error.add(1, &[]);
                return Err(ApplicationError::NonRetryable(err.to_string().into()));
            }
        }
    }
}
