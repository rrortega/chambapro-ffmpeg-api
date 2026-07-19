pub fn init_otel_tracer<S>(
    endpoint: &str,
    api_key: Option<&str>,
) -> Result<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>, anyhow::Error>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry_otlp::WithExportConfig;
    use std::collections::HashMap;

    let mut headers = HashMap::new();
    if let Some(key) = api_key {
        headers.insert("x-otlp-api-key".to_string(), key.to_string());
        headers.insert("api-key".to_string(), key.to_string());
        if key.contains('=') {
            for part in key.split(',') {
                let kv: Vec<&str> = part.split('=').collect();
                if kv.len() == 2 {
                    headers.insert(kv[0].trim().to_string(), kv[1].trim().to_string());
                }
            }
        }
    }

    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint(endpoint)
        .with_headers(headers);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            opentelemetry_sdk::trace::config().with_resource(
                opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "chambapro-ffmpeg-api"),
                ])
            )
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}
