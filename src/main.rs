use lambda_extension::{
    service_fn, Error, Extension, LambdaTelemetry, LambdaTelemetryRecord, SharedService,
};
use opentelemetry::{
    global,
    trace::{Span, TraceContextExt, TraceError, Tracer},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use tracing::{debug, error, info, warn};
use tracing_subscriber;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Config {
    collector_endpoint: String,
    service_name: String,
}

fn load_config() -> Config {
    let config = Config {
        collector_endpoint: env::var("COLLECTOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".to_string()),
        service_name: env::var("SERVICE_NAME").unwrap_or_else(|_| "lambda_extension".to_string()),
    };
    debug!("Loaded configuration: {:?}", config);
    config
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    info!("Lambda Extension starting up");

    let config = load_config();
    info!("Loaded configuration: {:?}", config);

    let tracer_provider = init_opentelemetry(&config).expect("failed to initialize opentelemetry");
    global::set_tracer_provider(tracer_provider);

    let telemetry_processor = SharedService::new(service_fn(handler));

    info!("Starting Lambda Extension");
    let extension_result = Extension::new()
        .with_telemetry_processor(telemetry_processor)
        .run()
        .await;

    info!("Lambda Extension shutting down");
    global::shutdown_tracer_provider();
}

fn init_opentelemetry(config: &Config) -> Result<sdktrace::TracerProvider, TraceError> {
    info!(
        "Initializing OpenTelemetry with endpoint: {}",
        config.collector_endpoint
    );
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(&config.collector_endpoint),
        )
        .with_trace_config(
            sdktrace::config().with_resource(opentelemetry_sdk::Resource::new(vec![
                opentelemetry::KeyValue::new("service.name", config.service_name.clone()),
            ])),
        )
        .install_batch(runtime::Tokio)?;

    info!("OpenTelemetry initialized successfully");
    Ok(provider)
}
async fn handler(events: Vec<LambdaTelemetry>) -> Result<(), Error> {
    debug!("Handler received {} events", events.len());
    let tracer = global::tracer("lambda_extension");

    tracer.in_span("handler", |cx| {
        let span = cx.span();
        for event in events {
            match event.record {
                LambdaTelemetryRecord::Function(record) => {
                    info!("Function log received");
                    let attributes = parse_function_log(&record);
                    span.add_event("function_log", attributes);
                }
                LambdaTelemetryRecord::PlatformInitStart {
                    initialization_type,
                    phase,
                    runtime_version,
                    runtime_version_arn,
                } => {
                    info!("Platform init event: {:?}", initialization_type);
                    span.add_event(
                        "init_start".to_string(),
                        vec![
                            KeyValue::new("init_type", format!("{:?}", initialization_type)),
                            KeyValue::new("phase", format!("{:?}", phase)),
                            KeyValue::new("runtime_version", format!("{:?}", runtime_version)),
                            KeyValue::new(
                                "runtime_version_arn",
                                format!("{:?}", runtime_version_arn),
                            ),
                        ],
                    );
                }
                LambdaTelemetryRecord::PlatformInitRuntimeDone {
                    initialization_type,
                    phase,
                    ..
                } => {
                    info!("Platform init done: {:?}", initialization_type);
                    span.add_event(
                        "init_runtime_done".to_string(),
                        vec![
                            KeyValue::new("init_type", format!("{:?}", initialization_type)),
                            KeyValue::new("phase", format!("{:?}", phase)),
                        ],
                    )
                }
                LambdaTelemetryRecord::PlatformInitReport {
                    initialization_type,
                    metrics,
                    phase,
                    ..
                } => {
                    info!("Platform init report: {:?}", initialization_type);
                    span.add_event(
                        "init_report".to_string(),
                        vec![
                            KeyValue::new("init_type", format!("{:?}", initialization_type)),
                            KeyValue::new("phase", format!("{:?}", phase)),
                            KeyValue::new("duration", format!("{:?}", metrics.duration_ms)),
                        ],
                    );
                }
                LambdaTelemetryRecord::PlatformStart { request_id, .. } => {
                    info!("Platform start event: {:?}", request_id);
                    span.add_event(
                        "platform_start".to_string(),
                        vec![KeyValue::new("request_id", format!("{:?}", request_id))],
                    );
                }
                LambdaTelemetryRecord::PlatformRuntimeDone {
                    metrics,
                    request_id,
                    ..
                } => {
                    info!("Platform runtime done: {:?}", request_id);
                    span.add_event(
                        "runtime_done",
                        vec![
                            KeyValue::new("request_id", format!("{:?}", request_id)),
                            KeyValue::new("duration", format!("{:?}", metrics)),
                        ],
                    )
                }
                LambdaTelemetryRecord::PlatformReport {
                    metrics,
                    request_id,
                    ..
                } => {
                    info!("Platform report event: {:?}", request_id);
                    span.add_event(
                        "platform_report".to_string(),
                        vec![
                            KeyValue::new("request_id", format!("{:?}", request_id)),
                            KeyValue::new("duration", format!("{:?}", metrics.duration_ms)),
                        ],
                    );
                }
                _ => {
                    info!("Unhandled event: {:?}", event);
                    span.add_event(
                        "unhandled_event".to_string(),
                        vec![KeyValue::new("event", format!("{:?}", event))],
                    );
                }
            }
        }
    });

    Ok(())
}

fn parse_function_log(record: &str) -> Vec<KeyValue> {
    if let Some(json_start) = record.find('{') {
        let json_str = &record[json_start..];
        match serde_json::from_str::<Value>(json_str) {
            Ok(json) => {
                let mut event_attributes = Vec::new();
                if let Value::Object(map) = json {
                    for (key, value) in map {
                        let value_str = match value {
                            Value::String(s) => s,
                            _ => value.to_string(),
                        };
                        event_attributes.push(KeyValue::new(key, value_str));
                    }
                }
                event_attributes
            }
            Err(e) => {
                warn!("Failed to parse JSON from function log: {:?}", e);
                vec![
                    KeyValue::new("error", format!("{:?}", e)),
                    KeyValue::new("raw_log", record.to_string()),
                ]
            }
        }
    } else {
        warn!("No JSON found in function log");
        vec![KeyValue::new("raw_log", record.to_string())]
    }
}
