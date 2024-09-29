use lambda_extension::{
    service_fn, Error, Extension, LambdaTelemetry, LambdaTelemetryRecord, SharedService,
};
use opentelemetry::{
    global,
    trace::{TraceError, Tracer},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Config {
    collector_endpoint: String,
    service_name: String,
}

fn load_config() -> Config {
    Config {
        collector_endpoint: env::var("COLLECTOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".to_string()),
        service_name: env::var("SERVICE_NAME").unwrap_or_else(|_| "lambda_extension".to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = load_config();
    let tracer_provider =
        init_opentelemetry(&config).expect("Failed to initialize tracer provider");
    global::set_tracer_provider(tracer_provider);

    let telemetry_processor = SharedService::new(service_fn(handler));

    Extension::new()
        .with_telemetry_processor(telemetry_processor)
        .run()
        .await
}

fn init_opentelemetry(config: &Config) -> Result<sdktrace::TracerProvider, TraceError> {
    opentelemetry_otlp::new_pipeline()
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
        .install_batch(runtime::Tokio)
}

async fn handler(events: Vec<LambdaTelemetry>) -> Result<(), Error> {
    for event in events {
        match event.record {
            LambdaTelemetryRecord::Function(record) => {
                println!("Function log: {}", record);
            }
            LambdaTelemetryRecord::PlatformInitStart {
                initialization_type,
                phase,
                runtime_version,
                runtime_version_arn,
            } => {
                println!("Platform Init Start:");
                println!("  Initialization Type: {:?}", initialization_type);
                println!("  Phase: {:?}", phase);
                println!("  Runtime Version: {:?}", runtime_version);
                println!("  Runtime Version ARN: {:?}", runtime_version_arn);
            }
            LambdaTelemetryRecord::PlatformInitRuntimeDone {
                initialization_type,
                phase,
                ..
            } => {
                println!("Platform Init Runtime Done:");
                println!("  Initialization Type: {:?}", initialization_type);
                println!("  Phase: {:?}", phase);
            }
            LambdaTelemetryRecord::PlatformInitReport {
                initialization_type,
                metrics,
                phase,
                ..
            } => {
                println!("Platform Init Report:");
                println!("  Initialization Type: {:?}", initialization_type);
                println!("  Phase: {:?}", phase);
                println!("  Duration: {} ms", metrics.duration_ms);
            }
            LambdaTelemetryRecord::PlatformStart { request_id, .. } => {
                println!("Platform Start: {}", request_id);
            }
            LambdaTelemetryRecord::PlatformRuntimeDone {
                metrics,
                request_id,
                ..
            } => {
                println!("Platform Runtime Done:");
                println!("  Request ID: {:?}", request_id);
                println!("  Duration: {:?} ms", metrics);
            }
            LambdaTelemetryRecord::PlatformReport {
                metrics,
                request_id,
                ..
            } => {
                println!("Platform Report:");
                println!("  Request ID: {:?}", request_id);
                println!("  Duration: {} ms", metrics.duration_ms);
            }
            _ => {
                println!("Unhandled event: {:?}", event);
            }
        }
    }

    Ok(())
}
