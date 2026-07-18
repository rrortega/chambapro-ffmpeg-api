use axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::Path};
use tokio::{fs::File, io::AsyncWriteExt, process::Command};
use tokio_util::io::ReaderStream;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let api_key = std::env::var("API_KEY").ok().filter(|s| !s.trim().is_empty());
    if api_key.is_some() {
        info!("API Key authentication is enabled");
    } else {
        info!("API Key authentication is disabled (no API_KEY env var provided)");
    }

    let storage_dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".to_string());
    tokio::fs::create_dir_all(&storage_dir).await?;
    info!("Storage directory set to: {}", storage_dir);

    let host_url = std::env::var("HOST_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
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
        tokio::spawn(async move {
            if let Err(e) = run_queue_workers(
                manager_clone,
                http_client,
                storage_dir_clone,
                host_url_clone,
                max_retries,
                cleanup_hours,
            ).await {
                error!("Queue worker loop error: {:?}", e);
            }
        });
    } else {
        info!("Redis URL not configured. Queue-based background processing disabled.");
    }

    let state = AppState {
        http_client: Client::new(),
        api_key,
        redis_manager,
        storage_dir,
        host_url,
        max_retries,
        cleanup_hours,
    };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/convert", post(convert_media))
        .route("/convert-async", post(convert_media_async))
        .route("/download/:file_name", get(download_file_endpoint))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
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

async fn convert_media(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Response, AppError> {
    // API Key Authentication Guard
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
        // Clean up uploaded file if present
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

    // Call ffmpeg synchronously
    let ffmpeg_res = run_ffmpeg(Path::new(&input_path), Path::new(&out_path), &output_format).await;

    // Cleanup input file immediately after ffmpeg runs
    let _ = tokio::fs::remove_file(&input_path).await;

    // Check ffmpeg result
    ffmpeg_res?;

    // Stream the response back
    let file = File::open(&out_path).await?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    // Get the file size for content-length if possible
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

    // Spawn task to delete the output file on Unix as file descriptors can remain open
    let out_path_clone = out_path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        let _ = tokio::fs::remove_file(out_path_clone).await;
    });
    
    Ok(response)
}

async fn convert_media_async(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Response, AppError> {
    // API Key Authentication Guard
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

    // Route based on Redis availability
    if let Some(mut manager) = state.redis_manager {
        // Mode 2: Redis queueing enabled
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
        let client = state.http_client.clone();
        let storage_dir = state.storage_dir.clone();
        let uuid_clone = uuid.clone();
        tokio::spawn(async move {
            info!("Enqueued simple background task (No Redis) for UUID {}", uuid_clone);
            let out_path = format!("{}/{}.{}", storage_dir, uuid_clone, output_format);
            let res = run_ffmpeg(Path::new(&input_path), Path::new(&out_path), &output_format).await;
            let _ = tokio::fs::remove_file(&input_path).await;

            if let Err(e) = res {
                error!("Simple background conversion failed for UUID {}: {:?}", uuid_clone, e);
                let _ = send_simple_webhook_error(&client, &callback_url, &uuid_clone, &e.to_string()).await;
                return;
            }

            // Webhook payload: check if we should send file or just info
            let webhook_res = if include_file {
                send_webhook_with_file(&client, &callback_url, &uuid_clone, &out_path, &output_format).await
            } else {
                // Since this simple mode doesn't store files permanently (no automatic 24h cleanup without Redis),
                // we'll send it but warn that download endpoints need Redis, or just send success status.
                send_simple_webhook_success(&client, &callback_url, &uuid_clone, "success").await
            };

            if let Err(e) = webhook_res {
                error!("Simple background webhook failed for UUID {}: {:?}", uuid_clone, e);
            }

            // Clean up output file
            let _ = tokio::fs::remove_file(&out_path).await;
        });

        Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "uuid": uuid, "enqueue": true })),
        ).into_response())
    }
}

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

// Background queue worker execution loops
async fn run_queue_workers(
    mut manager: redis::aio::ConnectionManager,
    client: Client,
    storage_dir: String,
    host_url: String,
    max_retries: u32,
    cleanup_hours: u64,
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
                    } else {
                        info!("Webhook successfully delivered for UUID {}", uuid);
                    }
                }
                JobType::Cleanup { uuid, output_path } => {
                    info!("Running scheduled cleanup for UUID {} removing {:?}", uuid, output_path);
                    let _ = tokio::fs::remove_file(output_path).await;
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
