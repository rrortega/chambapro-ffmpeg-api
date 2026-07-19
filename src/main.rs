mod models;
mod telemetry;
mod routes;
mod dashboard;
mod worker;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use reqwest::Client;
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use utoipa::OpenApi;

use models::{AppState, DashboardLogWriter, SharedDashboardState, track_metrics};
use routes::{health_check, convert_media, convert_media_async, download_file_endpoint, admin_cleanup_endpoint, get_job_status};
use dashboard::{dashboard_page, dashboard_api, perform_directory_cleanup, load_dashboard_from_disk};
use worker::run_queue_workers;
use telemetry::init_otel_tracer;

#[derive(OpenApi)]
#[openapi(
    paths(
        routes::health_check,
        routes::convert_media,
        routes::convert_media_async,
        routes::download_file_endpoint,
        routes::admin_cleanup_endpoint,
        routes::get_job_status
    ),
    info(
        title = "Chambapro FFmpeg API",
        version = "1.0.0",
        description = "High-performance API for asynchronous and synchronous audio/video conversion using FFmpeg."
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let storage_dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".to_string());
    tokio::fs::create_dir_all(&storage_dir).await?;
    info!("Storage directory set to: {}", storage_dir);

    let loaded_state = load_dashboard_from_disk(&storage_dir).await;
    let dashboard_state = Arc::new(RwLock::new(loaded_state));

    let writer = DashboardLogWriter {
        state: dashboard_state.clone(),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(move || writer.clone());
    let env_filter = EnvFilter::from_default_env().add_directive("info".parse().unwrap());

    let telemetry_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .or_else(|_| std::env::var("TELEMETRY_ENDPOINT"))
        .ok()
        .filter(|s| !s.trim().is_empty());

    let telemetry_api_key = std::env::var("TELEMETRY_API_KEY")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_HEADERS"))
        .ok()
        .filter(|s| !s.trim().is_empty());

    let otel_layer = if let Some(endpoint) = telemetry_endpoint {
        info!("OpenTelemetry endpoint configured at: {}. Initializing OTLP trace pipeline...", endpoint);
        match init_otel_tracer(&endpoint, telemetry_api_key.as_deref()) {
            Ok(layer) => {
                info!("OpenTelemetry tracing layer successfully initialized.");
                Some(layer)
            }
            Err(e) => {
                warn!("Failed to initialize OpenTelemetry tracing layer: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    let api_key = std::env::var("API_KEY").ok().filter(|s| !s.trim().is_empty());
    if api_key.is_some() {
        info!("API Key authentication is enabled");
    } else {
        info!("API Key authentication is disabled (no API_KEY env var provided)");
    }

    let host_url = std::env::var("PUBLIC_URL")
        .or_else(|_| std::env::var("HOST_URL"))
        .unwrap_or_else(|_| "http://localhost:80".to_string());

    let max_retries = std::env::var("MAX_RETRIES")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(3);
    let cleanup_hours = std::env::var("CLEANUP_HOURS")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(24);

    let redis_url = std::env::var("REDIS_URL").ok().filter(|s| !s.trim().is_empty());
    let mut redis_manager = None;
    let shared_dashboard = SharedDashboardState(dashboard_state);

    if let Some(url) = redis_url {
        info!("Connecting to Redis at: {}", url);
        let client = redis::Client::open(url)?;
        let manager = redis::aio::ConnectionManager::new(client).await?;
        redis_manager = Some(manager.clone());

        let manager_clone = manager.clone();
        let http_client = Client::new();
        let storage_dir_clone = storage_dir.clone();
        let host_url_clone = host_url.clone();
        let dashboard_clone = shared_dashboard.clone();
        tokio::spawn(async move {
            if let Err(e) = run_queue_workers(
                manager_clone,
                http_client,
                storage_dir_clone,
                host_url_clone,
                max_retries,
                cleanup_hours,
                dashboard_clone,
            ).await {
                error!("Queue worker loop error: {:?}", e);
            }
        });
    } else {
        info!("Redis URL not configured. Queue-based background processing disabled.");
    }

    let storage_dir_cleanup = storage_dir.clone();
    let cleanup_hours_val = cleanup_hours;
    let dashboard_state_cleanup = shared_dashboard.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1800)).await;
            info!("Running periodic automatic directory cleanup scan...");
            if let Err(e) = perform_directory_cleanup(&storage_dir_cleanup, cleanup_hours_val, &dashboard_state_cleanup).await {
                error!("Periodic automatic directory cleanup failed: {:?}", e);
            }
        }
    });

    let state = AppState {
        http_client: Client::new(),
        api_key,
        redis_manager,
        storage_dir,
        host_url,
        max_retries,
        cleanup_hours,
        dashboard: shared_dashboard.clone(),
    };

    let app = Router::new()
        .route("/", get(|| async { axum::response::Redirect::permanent("/docs") }))
        .route("/health", get(health_check))
        .route("/convert", post(convert_media))
        .route("/convert-async", post(convert_media_async))
        .route("/download/:file_name", get(download_file_endpoint))
        .route("/admin/cleanup", post(admin_cleanup_endpoint))
        .route("/status/:uuid", get(get_job_status))
        .route("/dashboard", get(dashboard_page))
        .route("/api/dashboard", get(dashboard_api))
        .layer(middleware::from_fn_with_state(state.clone(), track_metrics))
        .merge(utoipa_swagger_ui::SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "80".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[tokio::test]
    async fn test_ffmpeg_wrapper_missing_file() {
        let res = worker::run_ffmpeg(Path::new("non_existent.oga"), Path::new("out.mp3"), "mp3").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_download_file_invalid_url() {
        let client = Client::new();
        let res = worker::download_file(&client, "http://invalid-url-12345.com", None, "out.tmp").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_status_endpoint_non_existent_uuid() {
        use axum::http::Request;
        use axum::body::Body;
        use tower::ServiceExt;

        let state = AppState {
            http_client: Client::new(),
            api_key: None,
            redis_manager: None,
            storage_dir: "./storage".to_string(),
            host_url: "http://localhost".to_string(),
            max_retries: 3,
            cleanup_hours: 24,
            dashboard: SharedDashboardState(Arc::new(RwLock::new(models::DashboardState {
                jobs: Vec::new(),
                logs: Vec::new(),
                metrics: Vec::new(),
            }))),
        };

        let app = Router::new()
            .route("/status/:uuid", get(get_job_status))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status/non-existent-uuid-1234")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }
}
