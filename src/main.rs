use axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    sync::{Arc, RwLock},
};
use tokio::{fs::File, io::AsyncWriteExt, process::Command};
use tokio_util::io::ReaderStream;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use uuid::Uuid;
use utoipa::OpenApi;
use chrono::TimeZone;

#[derive(Serialize, Clone, Debug, Deserialize)]
struct DashboardJob {
    uuid: String,
    job_type: String,
    status: String, // "Enqueued", "Processing", "Success", "Failed"
    retries: u32,
    error: Option<String>,
    timestamp: String,
}

#[derive(Serialize, Clone, Debug, Deserialize)]
struct RequestMetric {
    timestamp: String, // RFC3339 string
    duration_ms: u64,
    endpoint: String,
    status: u16,
}

struct DashboardState {
    jobs: Vec<DashboardJob>,
    logs: Vec<String>,
    metrics: Vec<RequestMetric>,
}

#[derive(Clone)]
struct SharedDashboardState(Arc<RwLock<DashboardState>>);

#[allow(dead_code)]
#[derive(Clone)]
struct AppState {
    http_client: Client,
    api_key: Option<String>,
    redis_manager: Option<redis::aio::ConnectionManager>,
    storage_dir: String,
    host_url: String,
    max_retries: u32,
    cleanup_hours: u64,
    dashboard: SharedDashboardState,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum JobType {
    Convert {
        uuid: String,
        input_path: String,
        output_format: String,
        callback_url: String,
        include_file: bool,
        retry_count: u32,
    },
    Webhook {
        uuid: String,
        callback_url: String,
        success: bool,
        error_message: Option<String>,
        output_path: Option<String>,
        output_format: String,
        include_file: bool,
    },
    Cleanup {
        uuid: String,
        output_path: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Job {
    id: String,
    job_type: JobType,
}

// Custom log writer to capture all logs to the dashboard in real-time
#[derive(Clone)]
struct DashboardLogWriter {
    state: Arc<RwLock<DashboardState>>,
}

impl std::io::Write for DashboardLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let msg = String::from_utf8_lossy(buf).to_string();
        if let Ok(mut state) = self.state.write() {
            state.logs.push(msg.clone());
            if state.logs.len() > 150 {
                state.logs.remove(0);
            }
        }
        std::io::stdout().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        health_check,
        convert_media,
        convert_media_async,
        download_file_endpoint,
        admin_cleanup_endpoint
    ),
    info(
        title = "Chambapro FFmpeg API",
        version = "1.0.0",
        description = "High-performance API for asynchronous and synchronous audio/video conversion using FFmpeg."
    )
)]
struct ApiDoc;

// Load persisted dashboard data from disk
async fn load_dashboard_from_disk(storage_dir: &str) -> DashboardState {
    let mut jobs = Vec::new();
    let mut metrics = Vec::new();

    // Ensure directory structures exist
    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    let _ = tokio::fs::create_dir_all(&jobs_dir).await;
    let _ = tokio::fs::create_dir_all(&metrics_dir).await;

    // Load Jobs
    if let Ok(mut entries) = tokio::fs::read_dir(&jobs_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().is_file() {
                if let Ok(content) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(job) = serde_json::from_str::<DashboardJob>(&content) {
                        jobs.push(job);
                    }
                }
            }
        }
    }
    jobs.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    // Load Metrics
    if let Ok(mut entries) = tokio::fs::read_dir(&metrics_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().is_file() {
                if let Ok(content) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(metric) = serde_json::from_str::<RequestMetric>(&content) {
                        metrics.push(metric);
                    }
                }
            }
        }
    }
    metrics.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    info!("Loaded {} jobs and {} request metrics from disk cache", jobs.len(), metrics.len());

    DashboardState {
        jobs,
        logs: Vec::new(),
        metrics,
    }
}

// Persist a job to disk
async fn save_job_to_disk(storage_dir: &str, job: &DashboardJob) {
    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    let path = format!("{}/{}.json", jobs_dir, job.uuid);
    if let Ok(content) = serde_json::to_string(job) {
        let _ = tokio::fs::write(path, content).await;
    }
}

// Persist a request metric to disk
async fn save_metric_to_disk(storage_dir: &str, metric: &RequestMetric) {
    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    let path = format!("{}/{}.json", metrics_dir, Uuid::new_v4());
    if let Ok(content) = serde_json::to_string(metric) {
        let _ = tokio::fs::write(path, content).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let storage_dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".to_string());
    tokio::fs::create_dir_all(&storage_dir).await?;
    info!("Storage directory set to: {}", storage_dir);

    // Load existing cache from storage folder before configuring logs/writers
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

        // Spawn the Redis background workers
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

    // Spawn automatic periodic directory cleanup task (runs every 30 minutes)
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

async fn track_metrics(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let start = std::time::Instant::now();
    let path = req.uri().path().to_string();

    let is_api_metric = !path.starts_with("/dashboard")
        && !path.starts_with("/api/dashboard")
        && !path.starts_with("/docs")
        && !path.starts_with("/api-docs");

    let response = next.run(req).await;

    if is_api_metric {
        let duration = start.elapsed().as_millis() as u64;
        let status = response.status().as_u16();
        
        let new_metric = RequestMetric {
            timestamp: chrono::Local::now().to_rfc3339(),
            duration_ms: duration,
            endpoint: path,
            status,
        };

        // Write to memory cache
        if let Ok(mut db_state) = state.dashboard.0.write() {
            db_state.metrics.push(new_metric.clone());
            if db_state.metrics.len() > 2000 {
                db_state.metrics.remove(0);
            }
        }

        // Write to disk in non-blocking background thread
        let storage_dir = state.storage_dir.clone();
        tokio::spawn(async move {
            save_metric_to_disk(&storage_dir, &new_metric).await;
        });
    }

    response
}

fn update_job_status(
    dashboard: &SharedDashboardState,
    uuid: String,
    job_type: &str,
    status: &str,
    retries: u32,
    error: Option<String>,
) {
    if let Ok(mut state) = dashboard.0.write() {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let job_updated;

        if let Some(job) = state.jobs.iter_mut().find(|j| j.uuid == uuid) {
            job.status = status.to_string();
            job.retries = retries;
            job.error = error.clone();
            job.timestamp = timestamp.clone();
            // Preserve the original formats if enqueued or processing
            if !job_type.contains("Webhook") && !job_type.contains("Cleanup") {
                job.job_type = job_type.to_string();
            }
            job_updated = Some(job.clone());
        } else {
            let new_job = DashboardJob {
                uuid,
                job_type: job_type.to_string(),
                status: status.to_string(),
                retries,
                error: error.clone(),
                timestamp,
            };
            state.jobs.push(new_job.clone());
            job_updated = Some(new_job);
            if state.jobs.len() > 500 {
                state.jobs.remove(0);
            }
        }

        // Write updated job to disk asynchronously
        if let Some(job) = job_updated {
            let storage_dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".to_string());
            tokio::spawn(async move {
                save_job_to_disk(&storage_dir, &job).await;
            });
        }
    }
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Server is healthy", body = String)
    )
)]
async fn health_check() -> &'static str {
    "OK"
}

// Clean up metrics and jobs files from disk older than 30 days
async fn perform_dashboard_disk_cleanup(storage_dir: &str) -> anyhow::Result<()> {
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(30 * 24 * 3600); // 30 days
    let mut cleaned_count = 0;

    // 1. Clean jobs
    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    if let Ok(mut entries) = tokio::fs::read_dir(&jobs_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Clean metrics
    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    if let Ok(mut entries) = tokio::fs::read_dir(&metrics_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if cleaned_count > 0 {
        info!("Dashboard retention cleanup: removed {} expired data cache files", cleaned_count);
    }
    Ok(())
}

async fn perform_directory_cleanup(
    storage_dir: &str,
    cleanup_hours: u64,
    dashboard: &SharedDashboardState,
) -> anyhow::Result<()> {
    let mut dir = tokio::fs::read_dir(storage_dir).await?;
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(cleanup_hours * 3600);
    let mut cleaned_count = 0;

    // Scan regular temp uploaded / converted files
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            if let Ok(metadata) = entry.metadata().await {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(age) = now.duration_since(modified) {
                        if age > max_age {
                            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                                let uuid_part = file_name.split('.').next().unwrap_or(file_name);
                                info!("Cleaning up expired file: {:?}", file_name);
                                if tokio::fs::remove_file(&path).await.is_ok() {
                                    cleaned_count += 1;
                                    update_job_status(
                                        dashboard,
                                        uuid_part.to_string(),
                                        "Cleanup (Auto)",
                                        "Success",
                                        0,
                                        None,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if cleaned_count > 0 {
        info!("Directory cleanup finished. Removed {} expired files.", cleaned_count);
    }

    let _ = perform_dashboard_disk_cleanup(storage_dir).await;

    // Clean up memory cache vectors to prevent endless leaks in RAM
    if let Ok(mut state) = dashboard.0.write() {
        let max_age_metrics = chrono::Duration::days(30);
        let now_time = chrono::Local::now();
        
        state.metrics.retain(|m| {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&m.timestamp) {
                let age = now_time.signed_duration_since(dt);
                age < max_age_metrics
            } else {
                true
            }
        });
        
        state.jobs.retain(|j| {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&j.timestamp, "%Y-%m-%d %H:%M:%S") {
                if let Some(dt_local) = chrono::Local.from_local_datetime(&dt).single() {
                    let age = now_time.signed_duration_since(dt_local);
                    age < max_age_metrics
                } else {
                    true
                }
            } else {
                true
            }
        });
    }

    Ok(())
}

#[utoipa::path(
    post,
    path = "/admin/cleanup",
    responses(
        (status = 200, description = "Directory cleanup triggered successfully", body = String),
        (status = 401, description = "Unauthorized - Missing or invalid API Key")
    ),
    params(
        ("x-api-key" = Option<String>, Header, description = "Optional API Key for authentication")
    )
)]
async fn admin_cleanup_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    if let Some(expected_key) = &state.api_key {
        let provided_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok());

        if provided_key != Some(expected_key.as_str()) {
            return Ok((
                StatusCode::UNAUTHORIZED,
                "Unauthorized: Missing or invalid X-API-KEY header",
            ).into_response());
        }
    }

    info!("Manual admin cleanup endpoint triggered");
    perform_directory_cleanup(&state.storage_dir, state.cleanup_hours, &state.dashboard).await?;

    Ok((StatusCode::OK, "Cleanup completed successfully").into_response())
}

// Custom error type for route handlers
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Error: {:?}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[utoipa::path(
    post,
    path = "/convert",
    responses(
        (status = 200, description = "Successful media conversion", body = Vec<u8>),
        (status = 400, description = "Invalid request or callback_url provided"),
        (status = 401, description = "Unauthorized - Missing or invalid API Key")
    ),
    params(
        ("x-api-key" = Option<String>, Header, description = "Optional API Key for authentication")
    )
)]
async fn convert_media(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Response, AppError> {
    if let Some(expected_key) = &state.api_key {
        let provided_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok());

        if provided_key != Some(expected_key.as_str()) {
            return Ok((
                StatusCode::UNAUTHORIZED,
                "Unauthorized: Missing or invalid X-API-KEY header",
            ).into_response());
        }
    }

    let mut input_file_opt: Option<String> = None;
    let mut url_opt: Option<String> = None;
    let mut headers_opt: Option<String> = None;
    let mut output_format = "mp3".to_string(); // default
    let mut has_callback = false;

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "file" => {
                let uuid = Uuid::new_v4().to_string();
                let file_path = format!("{}/upload_{}", state.storage_dir, uuid);
                let mut f = tokio::fs::File::create(&file_path).await?;
                
                while let Some(chunk) = field.chunk().await.unwrap_or(None) {
                    f.write_all(&chunk).await?;
                }
                f.flush().await?;
                input_file_opt = Some(file_path);
            }
            "url" => {
                if let Ok(url) = field.text().await {
                    url_opt = Some(url);
                }
            }
            "headers" => {
                if let Ok(h) = field.text().await {
                    headers_opt = Some(h);
                }
            }
            "callback_url" => {
                if let Ok(url) = field.text().await {
                    if !url.trim().is_empty() {
                        has_callback = true;
                    }
                }
            }
            "output_format" => {
                if let Ok(fmt) = field.text().await {
                    output_format = fmt;
                }
            }
            _ => {}
        }
    }

    if has_callback {
        if let Some(path) = input_file_opt {
            let _ = tokio::fs::remove_file(path).await;
        }
        return Ok((
            StatusCode::BAD_REQUEST,
            "Callback URL is not allowed on /convert. For asynchronous requests with webhook callbacks, use the /convert-async endpoint instead.",
        ).into_response());
    }

    // Determine input source
    let input_path = if let Some(path) = input_file_opt {
        path
    } else if let Some(url) = url_opt {
        let uuid = Uuid::new_v4().to_string();
        let path = format!("{}/download_{}", state.storage_dir, uuid);
        download_file(&state.http_client, &url, headers_opt.as_deref(), &path).await?;
        path
    } else {
        return Ok((StatusCode::BAD_REQUEST, "Missing 'file' or 'url' field").into_response());
    };

    let uuid = Uuid::new_v4().to_string();
    let out_path = format!("{}/{}.{}", state.storage_dir, uuid, output_format);

    let input_ext = Path::new(&input_path).extension().and_then(|s| s.to_str()).unwrap_or("unknown");
    let job_type_str = format!("Convert (Sync: {} -> {})", input_ext, output_format);

    update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Processing", 0, None);

    // Call ffmpeg synchronously
    let ffmpeg_res = run_ffmpeg(Path::new(&input_path), Path::new(&out_path), &output_format).await;

    let _ = tokio::fs::remove_file(&input_path).await;

    // Check ffmpeg result
    if let Err(e) = &ffmpeg_res {
        update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Failed", 0, Some(e.to_string()));
        ffmpeg_res?;
    }

    update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Success", 0, None);

    // Stream the response back
    let file = File::open(&out_path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let meta = tokio::fs::metadata(&out_path).await?;
    
    let content_type = match output_format.as_str() {
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "webm" => "video/webm",
        _ => "application/octet-stream",
    };

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"output.{}\"", output_format))
        .header(header::CONTENT_LENGTH, meta.len())
        .body(body)
        .unwrap();

    let out_path_clone = out_path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        let _ = tokio::fs::remove_file(out_path_clone).await;
    });
    
    Ok(response)
}

#[utoipa::path(
    post,
    path = "/convert-async",
    responses(
        (status = 202, description = "Conversion enqueued successfully", body = serde_json::Value),
        (status = 400, description = "Invalid request or missing parameters"),
        (status = 401, description = "Unauthorized - Missing or invalid API Key")
    ),
    params(
        ("x-api-key" = Option<String>, Header, description = "Optional API Key for authentication")
    )
)]
async fn convert_media_async(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Response, AppError> {
    if let Some(expected_key) = &state.api_key {
        let provided_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok());

        if provided_key != Some(expected_key.as_str()) {
            return Ok((
                StatusCode::UNAUTHORIZED,
                "Unauthorized: Missing or invalid X-API-KEY header",
            ).into_response());
        }
    }

    let mut input_file_opt: Option<String> = None;
    let mut url_opt: Option<String> = None;
    let mut headers_opt: Option<String> = None;
    let mut callback_url_opt: Option<String> = None;
    let mut output_format = "mp3".to_string(); // default
    let mut include_file = false;

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "file" => {
                let uuid = Uuid::new_v4().to_string();
                let file_path = format!("{}/upload_{}", state.storage_dir, uuid);
                let mut f = tokio::fs::File::create(&file_path).await?;
                
                while let Some(chunk) = field.chunk().await.unwrap_or(None) {
                    f.write_all(&chunk).await?;
                }
                f.flush().await?;
                input_file_opt = Some(file_path);
            }
            "url" => {
                if let Ok(url) = field.text().await {
                    url_opt = Some(url);
                }
            }
            "headers" => {
                if let Ok(h) = field.text().await {
                    headers_opt = Some(h);
                }
            }
            "callback_url" => {
                if let Ok(url) = field.text().await {
                    let trimmed = url.trim();
                    if !trimmed.is_empty() {
                        callback_url_opt = Some(trimmed.to_string());
                    }
                }
            }
            "include_file" | "include_binary" => {
                if let Ok(val) = field.text().await {
                    include_file = val.trim().to_lowercase() == "true";
                }
            }
            "output_format" => {
                if let Ok(fmt) = field.text().await {
                    output_format = fmt;
                }
            }
            _ => {}
        }
    }

    let callback_url = match callback_url_opt {
        Some(url) => url,
        None => return Ok((StatusCode::BAD_REQUEST, "Missing 'callback_url' field").into_response()),
    };

    // Determine input source
    let input_path = if let Some(path) = input_file_opt {
        path
    } else if let Some(url) = url_opt {
        let uuid = Uuid::new_v4().to_string();
        let path = format!("{}/download_{}", state.storage_dir, uuid);
        download_file(&state.http_client, &url, headers_opt.as_deref(), &path).await?;
        path
    } else {
        return Ok((StatusCode::BAD_REQUEST, "Missing 'file' or 'url' field").into_response());
    };

    let uuid = Uuid::new_v4().to_string();
    let input_ext = Path::new(&input_path).extension().and_then(|s| s.to_str()).unwrap_or("unknown");

    // Route based on Redis availability
    if let Some(mut manager) = state.redis_manager {
        // Mode 2: Redis queueing enabled
        let job_type_str = format!("Convert (Redis: {} -> {})", input_ext, output_format);
        update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Enqueued", 0, None);
        
        let job = Job {
            id: Uuid::new_v4().to_string(),
            job_type: JobType::Convert {
                uuid: uuid.clone(),
                input_path,
                output_format,
                callback_url,
                include_file,
                retry_count: 0,
            },
        };

        let serialized_job = serde_json::to_string(&job)?;
        let _: () = redis::Cmd::lpush("chambapro:queue", serialized_job)
            .query_async(&mut manager)
            .await?;

        Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "uuid": uuid, "enqueue": true })),
        ).into_response())
    } else {
        // Mode 1: No Redis - Simple asynchronous task spawning
        let job_type_str = format!("Convert (Simple Async: {} -> {})", input_ext, output_format);
        update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Processing", 0, None);
        
        let client = state.http_client.clone();
        let storage_dir = state.storage_dir.clone();
        let dashboard = state.dashboard.clone();
        let uuid_clone = uuid.clone();
        
        tokio::spawn(async move {
            info!("Enqueued simple background task (No Redis) for UUID {}", uuid_clone);
            let out_path = format!("{}/{}.{}", storage_dir, uuid_clone, output_format);
            let res = run_ffmpeg(Path::new(&input_path), Path::new(&out_path), &output_format).await;
            let _ = tokio::fs::remove_file(&input_path).await;

            if let Err(e) = res {
                error!("Simple background conversion failed for UUID {}: {:?}", uuid_clone, e);
                update_job_status(&dashboard, uuid_clone.clone(), &job_type_str, "Failed", 0, Some(e.to_string()));
                let _ = send_simple_webhook_error(&client, &callback_url, &uuid_clone, &e.to_string()).await;
                return;
            }

            update_job_status(&dashboard, uuid_clone.clone(), &job_type_str, "Success", 0, None);

            let webhook_res = if include_file {
                send_webhook_with_file(&client, &callback_url, &uuid_clone, &out_path, &output_format).await
            } else {
                send_simple_webhook_success(&client, &callback_url, &uuid_clone, "success").await
            };

            let is_err = webhook_res.is_err();
            if let Err(e) = webhook_res {
                error!("Simple background webhook failed for UUID {}: {:?}", uuid_clone, e);
            }

            if include_file || is_err {
                let _ = tokio::fs::remove_file(&out_path).await;
            }
        });

        Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "uuid": uuid, "enqueue": true })),
        ).into_response())
    }
}

#[utoipa::path(
    get,
    path = "/download/{file_name}",
    responses(
        (status = 200, description = "Download converted file", body = Vec<u8>),
        (status = 404, description = "File not found or has been cleaned up")
    ),
    params(
        ("file_name" = String, Path, description = "The name of the file to download (e.g. uuid.ext)")
    )
)]
async fn download_file_endpoint(
    State(state): State<AppState>,
    AxumPath(file_name): AxumPath<String>,
) -> Result<Response, AppError> {
    let file_path = format!("{}/{}", state.storage_dir, file_name);
    let path = Path::new(&file_path);

    if !path.exists() || !path.is_file() {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "File has been cleaned up or does not exist" })),
        ).into_response());
    }

    let file = File::open(&file_path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    let content_type = match ext {
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "webm" => "video/webm",
        _ => "application/octet-stream",
    };

    let meta = tokio::fs::metadata(&file_path).await?;
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", file_name))
        .header(header::CONTENT_LENGTH, meta.len())
        .body(body)
        .unwrap();

    Ok(response)
}

async fn dashboard_page() -> Html<String> {
    Html(r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chambapro FFmpeg API - Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    <style>
        :root {
            --bg-base: #0b0d13;
            --bg-surface: rgba(20, 24, 38, 0.6);
            --border-glow: rgba(99, 102, 241, 0.2);
            --primary: #6366f1;
            --primary-glow: rgba(99, 102, 241, 0.4);
            --success: #10b981;
            --success-glow: rgba(16, 185, 129, 0.2);
            --error: #ef4444;
            --error-glow: rgba(239, 68, 68, 0.2);
            --warning: #f59e0b;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background-color: var(--bg-base);
            color: var(--text-main);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            padding: 2rem;
            padding-bottom: 5rem;
            background-image: radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
                              radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 40%);
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            padding-bottom: 1.5rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }

        h1 {
            font-size: 2.2rem;
            font-weight: 700;
            background: linear-gradient(135deg, #a5b4fc, #818cf8, #6366f1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .badge-live {
            background: var(--success-glow);
            color: var(--success);
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.35rem;
            border: 1px solid rgba(16, 185, 129, 0.3);
            box-shadow: 0 0 10px var(--success-glow);
        }

        .badge-live::before {
            content: '';
            display: inline-block;
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 0.6; }
            50% { transform: scale(1.2); opacity: 1; }
            100% { transform: scale(0.9); opacity: 0.6; }
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .stat-card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 16px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            transition: transform 0.3s ease, border-color 0.3s ease;
        }

        .stat-card:hover {
            transform: translateY(-2px);
            border-color: var(--primary-glow);
        }

        .stat-label {
            font-size: 0.9rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .stat-value {
            font-size: 2.2rem;
            font-weight: 700;
        }

        /* KPI Subgrid section */
        .kpis-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .kpi-card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 16px;
            padding: 1.25rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .kpi-title {
            font-size: 1rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .kpi-bar-row {
            margin-bottom: 0.75rem;
        }

        .kpi-bar-label {
            display: flex;
            justify-content: space-between;
            font-size: 0.85rem;
            margin-bottom: 0.25rem;
            font-family: 'JetBrains Mono', monospace;
        }

        .kpi-bar-outer {
            width: 100%;
            height: 6px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 9999px;
            overflow: hidden;
        }

        .kpi-bar-inner {
            height: 100%;
            background: var(--primary);
            border-radius: 9999px;
            width: 0%;
            transition: width 0.5s ease;
        }

        .grid-layout {
            display: grid;
            grid-template-columns: 1.3fr 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }

        @media (max-width: 1024px) {
            .grid-layout {
                grid-template-columns: 1fr;
            }
        }

        .card {
            background: var(--bg-surface);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 20px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            display: flex;
            flex-direction: column;
            margin-bottom: 2rem;
        }

        .card-header {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.25rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .table-container {
            overflow-y: auto;
            max-height: 400px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }

        th {
            padding: 0.75rem 1rem;
            font-size: 0.85rem;
            color: var(--text-muted);
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
            font-weight: 600;
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            font-size: 0.9rem;
            font-family: 'JetBrains Mono', monospace;
        }

        tr:hover td {
            background: rgba(255, 255, 255, 0.02);
        }

        .status-badge {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-enqueued { background: rgba(99, 102, 241, 0.15); color: #818cf8; }
        .status-processing { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
        .status-success { background: rgba(16, 185, 129, 0.15); color: #34d399; }
        .status-failed { background: rgba(239, 68, 68, 0.15); color: #f87171; }

        /* Logs Drawer Styling */
        .drawer-toggle-btn {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: rgba(20, 24, 38, 0.95);
            border-top: 1px solid var(--primary-glow);
            padding: 1rem;
            text-align: center;
            cursor: pointer;
            z-index: 100;
            font-weight: 600;
            color: #818cf8;
            box-shadow: 0 -5px 20px rgba(0,0,0,0.5);
            transition: background 0.3s;
        }

        .drawer-toggle-btn:hover {
            background: rgba(30, 36, 56, 0.98);
        }

        .drawer {
            position: fixed;
            bottom: -500px;
            left: 0;
            right: 0;
            height: 450px;
            background: #090b10;
            border-top: 1px solid var(--primary-glow);
            box-shadow: 0 -10px 40px rgba(0,0,0,0.8);
            z-index: 101;
            transition: bottom 0.4s cubic-bezier(0.16, 1, 0.3, 1);
            display: flex;
            flex-direction: column;
            padding: 1.5rem;
        }

        .drawer.open {
            bottom: 0;
        }

        .drawer-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
        }

        .drawer-close-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: var(--text-main);
            padding: 0.3rem 0.8rem;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.85rem;
            transition: background 0.3s;
        }

        .drawer-close-btn:hover {
            background: rgba(255, 255, 255, 0.1);
        }

        .terminal {
            flex-grow: 1;
            background: #050608;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 1rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            line-height: 1.5;
            overflow-y: auto;
            color: #d1d5db;
        }

        .log-line {
            margin-bottom: 0.35rem;
            word-break: break-all;
        }

        .log-info { color: #818cf8; }
        .log-warn { color: #fbbf24; }
        .log-error { color: #f87171; }

        select.chart-selector {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.15);
            color: var(--text-main);
            padding: 0.4rem 0.8rem;
            border-radius: 8px;
            font-family: inherit;
            cursor: pointer;
        }

        select.chart-selector:focus {
            outline: none;
            border-color: var(--primary);
        }
    </style>
</head>
<body>

    <header>
        <h1>Chambapro FFmpeg API 🚀</h1>
        <div class="badge-live">LIVE FEED</div>
    </header>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total Jobs</div>
            <div id="stat-total" class="stat-value">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Processing</div>
            <div id="stat-processing" class="stat-value" style="color: var(--warning);">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Success</div>
            <div id="stat-success" class="stat-value" style="color: var(--success);">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Failed</div>
            <div id="stat-failed" class="stat-value" style="color: var(--error);">0</div>
        </div>
    </div>

    <!-- KPI Section -->
    <div class="kpis-grid">
        <!-- KPI 1: Execution Mode Split -->
        <div class="kpi-card">
            <div class="kpi-title">Execution Mode (Requests) <span style="font-size: 0.8rem; color:#818cf8;">Sync vs Async</span></div>
            <div id="kpi-modes-container">
                <!-- Dynamic bars -->
            </div>
        </div>

        <!-- KPI 2: Webhooks Delivery metrics -->
        <div class="kpi-card">
            <div class="kpi-title">Webhooks Processed <span id="kpi-webhook-rate" style="font-size: 0.95rem; color:#10b981; font-weight:700;">100% Ok</span></div>
            <div id="kpi-webhooks-container">
                <!-- Dynamic bars -->
            </div>
        </div>

        <!-- KPI 3: Top Format Pairs -->
        <div class="kpi-card">
            <div class="kpi-title">Top Format Pairs <span style="font-size: 0.8rem; color:#818cf8;">Input → Output</span></div>
            <div id="kpi-pairs-container">
                <!-- Dynamic bars -->
            </div>
        </div>
    </div>

    <!-- Charts Layout Grid -->
    <div class="grid-layout">
        <!-- Performance Metric Chart -->
        <div class="card" style="height: 380px;">
            <div class="card-header">
                <span>API Traffic & Latency</span>
                <select id="granularity" class="chart-selector" onchange="updateMetricChart()">
                    <option value="minute">Minute</option>
                    <option value="hour" selected>Hour</option>
                    <option value="day">Day</option>
                </select>
            </div>
            <div id="metric-chart" style="height: 280px;"></div>
        </div>

        <!-- Monthly Activity Heatmap -->
        <div class="card" style="height: 380px;">
            <div class="card-header">
                <span>Activity (GitHub style)</span>
            </div>
            <div id="heatmap-chart" style="height: 280px;"></div>
        </div>
    </div>

    <!-- Recent Processes Panel (Full width) -->
    <div class="card">
        <div class="card-header">
            <span>Recent Processes</span>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>UUID</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Retries</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody id="jobs-tbody">
                    <!-- Dynamic content -->
                </tbody>
            </table>
        </div>
    </div>

    <!-- Bottom Sticky Toggle Button for stdout -->
    <div id="toggle-drawer-btn" class="drawer-toggle-btn" onclick="openDrawer()">
        📁 Show Live stdout & process logs
    </div>

    <!-- Terminal Logs Drawer (slides from bottom) -->
    <div id="logs-drawer" class="drawer">
        <div class="drawer-header">
            <span style="font-weight: 600; font-size: 1.1rem; color: #818cf8;">stdout & process logs</span>
            <button class="drawer-close-btn" onclick="closeDrawer()">Collapse Drawer ✕</button>
        </div>
        <div id="log-terminal" class="terminal">
            <!-- Dynamic content -->
        </div>
    </div>

    <script>
        let metricChartObj = null;
        let heatmapChartObj = null;
        let cachedMetrics = [];
        let cachedJobs = [];

        function openDrawer() {
            document.getElementById('logs-drawer').classList.add('open');
            document.getElementById('toggle-drawer-btn').style.display = 'none';
            const term = document.getElementById('log-terminal');
            term.scrollTop = term.scrollHeight;
        }

        function closeDrawer() {
            document.getElementById('logs-drawer').classList.remove('open');
            setTimeout(() => {
                document.getElementById('toggle-drawer-btn').style.display = 'block';
            }, 300);
        }

        function updateMetricChart() {
            const granularity = document.getElementById('granularity').value;
            const buckets = {};

            // Group metrics by granularity
            cachedMetrics.forEach(m => {
                const date = new Date(m.timestamp);
                let key = '';

                if (granularity === 'minute') {
                    key = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
                } else if (granularity === 'hour') {
                    key = `${date.getHours().toString().padStart(2, '0')}:00`;
                } else { // day
                    key = `${date.getMonth() + 1}/${date.getDate()}`;
                }

                if (!buckets[key]) {
                    buckets[key] = { count: 0, total_duration: 0 };
                }
                buckets[key].count += 1;
                buckets[key].total_duration += m.duration_ms;
            });

            // Get last 15 sorted categories
            const sortedKeys = Object.keys(buckets).sort().slice(-15);
            const counts = sortedKeys.map(k => buckets[k].count);
            const avgDurations = sortedKeys.map(k => Math.round(buckets[k].total_duration / buckets[k].count));

            const options = {
                series: [
                    { name: 'Requests', type: 'column', data: counts },
                    { name: 'Avg Latency (ms)', type: 'line', data: avgDurations }
                ],
                chart: {
                    height: 280,
                    type: 'line',
                    toolbar: { show: false },
                    background: 'transparent'
                },
                theme: { mode: 'dark' },
                stroke: { width: [0, 3], curve: 'smooth' },
                colors: ['#6366f1', '#10b981'],
                dataLabels: { enabled: false },
                labels: sortedKeys,
                yaxis: [
                    { title: { text: 'Requests' } },
                    { opposite: true, title: { text: 'Latency (ms)' } }
                ],
                grid: { borderColor: 'rgba(255,255,255,0.05)' }
            };

            if (metricChartObj) {
                metricChartObj.updateOptions(options);
            } else {
                metricChartObj = new ApexCharts(document.getElementById('metric-chart'), options);
                metricChartObj.render();
            }
        }

        function updateHeatmap() {
            const now = new Date();
            const daysData = {};
            
            // Initialize last 30 days to 0
            for (let i = 29; i >= 0; i--) {
                const d = new Date();
                d.setDate(now.getDate() - i);
                const dayKey = `${d.getFullYear()}-${(d.getMonth() + 1).toString().padStart(2, '0')}-${d.getDate().toString().padStart(2, '0')}`;
                daysData[dayKey] = 0;
            }

            // Aggregate metrics (traffic activity) per day
            cachedMetrics.forEach(m => {
                const date = new Date(m.timestamp);
                const dayKey = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}`;
                if (daysData[dayKey] !== undefined) {
                    daysData[dayKey] += 1;
                }
            });

            // Group into 7 rows for days of the week: Mon, Tue, Wed, Thu, Fri, Sat, Sun
            const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
            const series = daysOfWeek.map((dayName, idx) => {
                const data = [];
                for (let week = 0; week < 5; week++) {
                    const d = new Date();
                    d.setDate(now.getDate() - (4 - week) * 7 + (idx - now.getDay()));
                    const dayKey = `${d.getFullYear()}-${(d.getMonth() + 1).toString().padStart(2, '0')}-${d.getDate().toString().padStart(2, '0')}`;
                    const count = daysData[dayKey] || 0;
                    data.push({ x: `W${week+1}`, y: count });
                }
                return { name: dayName, data: data };
            });

            const options = {
                series: series,
                chart: {
                    height: 280,
                    type: 'heatmap',
                    toolbar: { show: false }
                },
                theme: { mode: 'dark' },
                dataLabels: { enabled: false },
                // Custom GitHub contributions colors (green gradient)
                plotOptions: {
                    heatmap: {
                        shadeIntensity: 0.5,
                        radius: 2,
                        useFillColorAsStroke: true,
                        colorScale: {
                            ranges: [
                                { from: 0, to: 0, name: 'No activity', color: '#161b22' },
                                { from: 1, to: 3, name: 'Low', color: '#0e4429' },
                                { from: 4, to: 7, name: 'Medium', color: '#006d32' },
                                { from: 8, to: 12, name: 'High', color: '#26a641' },
                                { from: 13, to: 1000, name: 'Very High', color: '#39d353' }
                            ]
                        }
                    }
                }
            };

            if (heatmapChartObj) {
                heatmapChartObj.updateOptions(options);
            } else {
                heatmapChartObj = new ApexCharts(document.getElementById('heatmap-chart'), options);
                heatmapChartObj.render();
            }
        }

        // Aggregate and update KPI cards
        function updateKPIs() {
            // KPI 1: Execution Mode Split
            let syncCount = cachedMetrics.filter(m => m.endpoint === '/convert').length;
            let asyncCount = cachedMetrics.filter(m => m.endpoint === '/convert-async').length;
            let totalRequests = syncCount + asyncCount || 1;

            let syncPercent = Math.round((syncCount / totalRequests) * 100);
            let asyncPercent = Math.round((asyncCount / totalRequests) * 100);

            document.getElementById('kpi-modes-container').innerHTML = `
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Synchronous (/convert)</span><span>${syncCount} (${syncPercent}%)</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${syncPercent}%; background:#6366f1;"></div></div>
                </div>
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Asynchronous (/convert-async)</span><span>${asyncCount} (${asyncPercent}%)</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${asyncPercent}%; background:#10b981;"></div></div>
                </div>
            `;

            // KPI 2: Webhooks Processed
            let webhooks = cachedJobs.filter(j => j.job_type === 'Webhook');
            let successWebhooks = webhooks.filter(j => j.status === 'Success').length;
            let failedWebhooks = webhooks.filter(j => j.status === 'Failed').length;
            let totalWebhooks = webhooks.length || 1;

            let successRate = Math.round((successWebhooks / totalWebhooks) * 100);
            document.getElementById('kpi-webhook-rate').innerText = `${successRate}% Ok`;
            if (successRate < 90) {
                document.getElementById('kpi-webhook-rate').style.color = 'var(--error)';
            } else {
                document.getElementById('kpi-webhook-rate').style.color = 'var(--success)';
            }

            let webOkPercent = Math.round((successWebhooks / totalWebhooks) * 100);
            let webFailPercent = Math.round((failedWebhooks / totalWebhooks) * 100);

            document.getElementById('kpi-webhooks-container').innerHTML = `
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Webhook Deliveries Success</span><span>${successWebhooks}</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${webOkPercent}%; background:#10b981;"></div></div>
                </div>
                <div class="kpi-bar-row">
                    <div class="kpi-bar-label"><span>Webhook Deliveries Failed</span><span>${failedWebhooks}</span></div>
                    <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${webFailPercent}%; background:#ef4444;"></div></div>
                </div>
            `;

            // KPI 3: Format Pairs
            let formatPairs = {};
            cachedJobs.forEach(j => {
                // Parse job_type (e.g. "Convert (Sync: oga -> mp3)")
                if (j.job_type.includes('->')) {
                    let parts = j.job_type.split(':');
                    if (parts.length > 1) {
                        let pair = parts[1].replace(')', '').trim();
                        formatPairs[pair] = (formatPairs[pair] || 0) + 1;
                    }
                }
            });

            // Sort and grab top 3
            let sortedPairs = Object.entries(formatPairs)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 3);

            let maxCount = sortedPairs.length > 0 ? sortedPairs[0][1] : 1;
            let pairsHTML = '';

            if (sortedPairs.length === 0) {
                pairsHTML = '<div class="kpi-bar-label" style="color:var(--text-muted);">No conversion pairs recorded yet.</div>';
            } else {
                sortedPairs.forEach(([pair, count]) => {
                    let pct = Math.round((count / maxCount) * 100);
                    pairsHTML += `
                        <div class="kpi-bar-row">
                            <div class="kpi-bar-label"><span>${pair}</span><span>${count} jobs</span></div>
                            <div class="kpi-bar-outer"><div class="kpi-bar-inner" style="width: ${pct}%; background:#818cf8;"></div></div>
                        </div>
                    `;
                });
            }
            document.getElementById('kpi-pairs-container').innerHTML = pairsHTML;
        }

        async function fetchDashboardData() {
            try {
                const response = await fetch('/api/dashboard');
                if (!response.ok) return;
                const data = await response.json();
                
                cachedMetrics = data.metrics || [];
                cachedJobs = data.jobs || [];

                // Update stats
                let total = data.jobs.length;
                let processing = data.jobs.filter(j => j.status === 'Processing').length;
                let success = data.jobs.filter(j => j.status === 'Success').length;
                let failed = data.jobs.filter(j => j.status === 'Failed').length;

                document.getElementById('stat-total').innerText = total;
                document.getElementById('stat-processing').innerText = processing;
                document.getElementById('stat-success').innerText = success;
                document.getElementById('stat-failed').innerText = failed;

                // Update Jobs Table
                const tbody = document.getElementById('jobs-tbody');
                tbody.innerHTML = '';
                data.jobs.reverse().forEach(job => {
                    const tr = document.createElement('tr');
                    const uuidShort = job.uuid.substring(0, 8) + '...';
                    const statusClass = 'status-' + job.status.toLowerCase();
                    
                    tr.innerHTML = `
                        <td title="${job.uuid}">${uuidShort}</td>
                        <td>${job.job_type}</td>
                        <td><span class="status-badge ${statusClass}">${job.status}</span></td>
                        <td>${job.retries}</td>
                        <td>${job.timestamp}</td>
                    `;
                    tbody.appendChild(tr);
                });

                // Update Terminal Logs
                const terminal = document.getElementById('log-terminal');
                const wasScrolledToBottom = terminal.scrollHeight - terminal.clientHeight <= terminal.scrollTop + 1;
                
                terminal.innerHTML = '';
                data.logs.forEach(line => {
                    const div = document.createElement('div');
                    div.className = 'log-line';
                    
                    if (line.includes('INFO')) {
                        div.classList.add('log-info');
                    } else if (line.includes('WARN')) {
                        div.classList.add('log-warn');
                    } else if (line.includes('ERROR')) {
                        div.classList.add('log-error');
                    }
                    
                    div.innerText = line.trim();
                    terminal.appendChild(div);
                });

                if (wasScrolledToBottom) {
                    terminal.scrollTop = terminal.scrollHeight;
                }

                // Render or update charts & KPIs
                updateMetricChart();
                updateHeatmap();
                updateKPIs();

            } catch (err) {
                console.error("Dashboard poll error:", err);
            }
        }

        setInterval(fetchDashboardData, 2000);
        fetchDashboardData();
    </script>
</body>
</html>"##.to_string())
}

async fn dashboard_api(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    if let Ok(data) = state.dashboard.0.read() {
        Ok(Json(serde_json::json!({
            "jobs": data.jobs,
            "logs": data.logs,
            "metrics": data.metrics,
        })))
    } else {
        Err(anyhow::anyhow!("Failed to read dashboard state").into())
    }
}

// Background queue worker execution loops
async fn run_queue_workers(
    mut manager: redis::aio::ConnectionManager,
    client: Client,
    storage_dir: String,
    host_url: String,
    max_retries: u32,
    cleanup_hours: u64,
    dashboard: SharedDashboardState,
) -> anyhow::Result<()> {
    info!("Starting Chambapro queue workers...");

    // Spawn a delayed scheduler loop to move delayed jobs (like cleanups) to the active queue
    let mut manager_delayed = manager.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Fetch any jobs scheduled for <= now
            let expired_jobs: Vec<String> = match redis::Cmd::zrangebyscore_limit(
                "chambapro:delayed",
                "-inf",
                now.to_string(),
                0,
                50,
            ).query_async(&mut manager_delayed).await {
                Ok(jobs) => jobs,
                Err(e) => {
                    error!("Error querying delayed jobs: {:?}", e);
                    continue;
                }
            };

            for job_str in expired_jobs {
                // Push to active queue
                let push_res: redis::RedisResult<()> = redis::pipe()
                    .lpush("chambapro:queue", &job_str)
                    .zrem("chambapro:delayed", &job_str)
                    .query_async(&mut manager_delayed)
                    .await;

                if let Err(e) = push_res {
                    error!("Failed to promote delayed job to active queue: {:?}", e);
                } else {
                    info!("Promoted delayed job to active queue successfully");
                }
            }
        }
    });

    // Main queue processor loop
    loop {
        // LPOP from active queue (equivalent to popping the job to execute)
        // Wait briefly if the queue is empty
        let popped: Option<String> = redis::Cmd::rpop("chambapro:queue", None)
            .query_async(&mut manager)
            .await?;

        let job_str = match popped {
            Some(s) => s,
            None => {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        let job: Job = match serde_json::from_str(&job_str) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to deserialize job: {:?}", e);
                continue;
            }
        };

        let mut manager_clone = manager.clone();
        let client_clone = client.clone();
        let storage_dir_clone = storage_dir.clone();
        let host_url_clone = host_url.clone();
        let dashboard_clone = dashboard.clone();

        tokio::spawn(async move {
            info!("Processing Job ID {} of type {:?}", job.id, job.job_type);
            match job.job_type {
                JobType::Convert {
                    uuid,
                    input_path,
                    output_format,
                    callback_url,
                    include_file,
                    retry_count,
                } => {
                    let input_ext = Path::new(&input_path).extension().and_then(|s| s.to_str()).unwrap_or("unknown");
                    let job_type_str = format!("Convert (Redis: {} -> {})", input_ext, output_format);
                    update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Processing", retry_count, None);
                    
                    let out_path = format!("{}/{}.{}", storage_dir_clone, uuid, output_format);
                    let conversion_res = run_ffmpeg(
                        Path::new(&input_path),
                        Path::new(&out_path),
                        &output_format,
                    ).await;

                    if let Err(e) = conversion_res {
                        let next_retry = retry_count + 1;
                        warn!("Conversion failed for UUID {} (attempt {}/{}): {:?}", uuid, next_retry, max_retries, e);
                        
                        if next_retry >= max_retries {
                            // Max retries reached, trigger failure webhook
                            update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Failed", max_retries, Some(e.to_string()));
                            let _ = tokio::fs::remove_file(&input_path).await;
                            let fail_job = Job {
                                id: Uuid::new_v4().to_string(),
                                job_type: JobType::Webhook {
                                    uuid,
                                    callback_url,
                                    success: false,
                                    error_message: Some(format!("Conversion failed after {} attempts: {:?}", max_retries, e)),
                                    output_path: None,
                                    output_format,
                                    include_file,
                                },
                            };
                            let _ = enqueue_job(&mut manager_clone, fail_job).await;
                        } else {
                            // Retry by enqueueing another conversion job
                            update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Processing", next_retry, Some(e.to_string()));
                            let retry_job = Job {
                                id: job.id,
                                job_type: JobType::Convert {
                                    uuid,
                                    input_path,
                                    output_format,
                                    callback_url,
                                    include_file,
                                    retry_count: next_retry,
                                },
                            };
                            let _ = enqueue_job(&mut manager_clone, retry_job).await;
                        }
                    } else {
                        // Success!
                        update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Success", retry_count, None);
                        let _ = tokio::fs::remove_file(&input_path).await;
                        
                        // 1. Enqueue Webhook job
                        let webhook_job = Job {
                            id: Uuid::new_v4().to_string(),
                            job_type: JobType::Webhook {
                                uuid: uuid.clone(),
                                callback_url,
                                success: true,
                                error_message: None,
                                output_path: Some(out_path.clone()),
                                output_format,
                                include_file,
                            },
                        };
                        let _ = enqueue_job(&mut manager_clone, webhook_job).await;

                        // 2. Schedule Cleanup job to run after cleanup_hours
                        let cleanup_job = Job {
                            id: Uuid::new_v4().to_string(),
                            job_type: JobType::Cleanup {
                                uuid,
                                output_path: out_path,
                            },
                        };
                        let delay_secs = cleanup_hours * 3600;
                        let run_at = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() + delay_secs;

                        let _ = enqueue_delayed_job(&mut manager_clone, cleanup_job, run_at).await;
                    }
                }
                JobType::Webhook {
                    uuid,
                    callback_url,
                    success,
                    error_message,
                    output_path,
                    output_format,
                    include_file,
                } => {
                    update_job_status(&dashboard_clone, uuid.clone(), "Webhook", "Processing", 0, None);
                    let webhook_res = if success {
                        if include_file {
                            if let Some(path) = &output_path {
                                send_webhook_with_file(&client_clone, &callback_url, &uuid, path, &output_format).await
                            } else {
                                send_simple_webhook_error(&client_clone, &callback_url, &uuid, "File path missing from successful webhook task").await
                            }
                        } else {
                            let download_url = format!("{}/download/{}.{}", host_url_clone, uuid, output_format);
                            let success_msg = format!("File converted successfully. Available for download for {} hours.", cleanup_hours);
                            client_clone.post(&callback_url)
                                .json(&serde_json::json!({
                                    "uuid": uuid,
                                    "success": true,
                                    "message": success_msg,
                                    "download_url": download_url
                                }))
                                .send()
                                .await
                                .map(|_| ())
                                .map_err(anyhow::Error::from)
                        }
                    } else {
                        let err_msg = error_message.unwrap_or_else(|| "Unknown conversion error".to_string());
                        send_simple_webhook_error(&client_clone, &callback_url, &uuid, &err_msg).await
                    };

                    if let Err(e) = webhook_res {
                        error!("Webhook delivery failed for Job ID {} targeting URL {}: {:?}", job.id, callback_url, e);
                        update_job_status(&dashboard_clone, uuid.clone(), "Webhook", "Failed", 0, Some(e.to_string()));
                    } else {
                        info!("Webhook successfully delivered for UUID {}", uuid);
                        update_job_status(&dashboard_clone, uuid.clone(), "Webhook", "Success", 0, None);
                    }
                }
                JobType::Cleanup { uuid, output_path } => {
                    info!("Running scheduled cleanup for UUID {} removing {:?}", uuid, output_path);
                    update_job_status(&dashboard_clone, uuid.clone(), "Cleanup", "Processing", 0, None);
                    let _ = tokio::fs::remove_file(output_path).await;
                    update_job_status(&dashboard_clone, uuid.clone(), "Cleanup", "Success", 0, None);
                }
            }
        });
    }
}

async fn enqueue_job(manager: &mut redis::aio::ConnectionManager, job: Job) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(&job)?;
    let _: () = redis::Cmd::lpush("chambapro:queue", serialized)
        .query_async(manager)
        .await?;
    Ok(())
}

async fn enqueue_delayed_job(
    manager: &mut redis::aio::ConnectionManager,
    job: Job,
    run_at: u64,
) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(&job)?;
    let _: () = redis::Cmd::zadd("chambapro:delayed", serialized, run_at)
        .query_async(manager)
        .await?;
    Ok(())
}

async fn download_file(
    client: &Client,
    url: &str,
    headers_json: Option<&str>,
    dest_path: &str,
) -> anyhow::Result<()> {
    let mut req = client.get(url);
    if let Some(h) = headers_json {
        let headers: HashMap<String, String> = serde_json::from_str(h)?;
        for (k, v) in headers {
            req = req.header(k, v);
        }
    }

    let mut res = req.send().await?;
    if !res.status().is_success() {
        anyhow::bail!("Failed to download file, status: {}", res.status());
    }

    let mut f = tokio::fs::File::create(dest_path).await?;
    while let Some(chunk) = res.chunk().await? {
        f.write_all(&chunk).await?;
    }
    f.flush().await?;

    Ok(())
}

async fn run_ffmpeg(input_path: &Path, output_path: &Path, format: &str) -> anyhow::Result<()> {
    info!("Running ffmpeg from {:?} to {:?} format {}", input_path, output_path, format);
    let output = Command::new("ffmpeg")
        .arg("-y")
        .arg("-i")
        .arg(input_path)
        .arg("-f")
        .arg(format)
        .arg(output_path)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("ffmpeg failed: {}", stderr);
        anyhow::bail!("ffmpeg conversion failed: {}", stderr);
    }
    info!("ffmpeg conversion successful");
    Ok(())
}

// Simple Webhook Helpers
async fn send_webhook_with_file(
    client: &Client,
    callback_url: &str,
    uuid: &str,
    file_path: &str,
    output_format: &str,
) -> anyhow::Result<()> {
    let file = File::open(file_path).await?;
    let stream = ReaderStream::new(file);
    let body = reqwest::Body::wrap_stream(stream);

    let file_part = reqwest::multipart::Part::stream(body)
        .file_name(format!("output.{}", output_format))
        .mime_str(match output_format {
            "mp3" => "audio/mpeg",
            "mp4" => "video/mp4",
            "wav" => "audio/wav",
            "ogg" => "audio/ogg",
            "webm" => "video/webm",
            _ => "application/octet-stream",
        })?;

    let form = reqwest::multipart::Form::new()
        .text("uuid", uuid.to_string())
        .part("file", file_part);

    let res = client.post(callback_url).multipart(form).send().await?;
    let status = res.status();
    if !status.is_success() {
        let err_body = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        anyhow::bail!("Webhook callback to {} failed (status {}): {}", callback_url, status, err_body);
    }
    Ok(())
}

async fn send_simple_webhook_success(
    client: &Client,
    callback_url: &str,
    uuid: &str,
    message: &str,
) -> anyhow::Result<()> {
    let res = client.post(callback_url)
        .json(&serde_json::json!({
            "uuid": uuid,
            "success": true,
            "message": message
        }))
        .send()
        .await?;
    
    let status = res.status();
    if !status.is_success() {
        let err_body = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        anyhow::bail!("Webhook success callback failed (status {}): {}", status, err_body);
    }
    Ok(())
}

async fn send_simple_webhook_error(
    client: &Client,
    callback_url: &str,
    uuid: &str,
    error_message: &str,
) -> anyhow::Result<()> {
    let res = client.post(callback_url)
        .json(&serde_json::json!({
            "uuid": uuid,
            "success": false,
            "error": error_message
        }))
        .send()
        .await?;
    
    let status = res.status();
    if !status.is_success() {
        let err_body = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        anyhow::bail!("Webhook error callback failed (status {}): {}", status, err_body);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ffmpeg_wrapper_missing_file() {
        let res = run_ffmpeg(Path::new("non_existent.oga"), Path::new("out.mp3"), "mp3").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_download_file_invalid_url() {
        let client = Client::new();
        let res = download_file(&client, "http://invalid-url-12345.com", None, "out.tmp").await;
        assert!(res.is_err());
    }
}

fn init_otel_tracer<S>(
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
