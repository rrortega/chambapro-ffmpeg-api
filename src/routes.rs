use crate::models::{AppError, AppState, Job, JobType, update_job_status};
use crate::worker::{download_file, enqueue_job, run_ffmpeg, send_simple_webhook_error, send_webhook_with_file, send_simple_webhook_success};
use crate::dashboard::perform_directory_cleanup;
use axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use reqwest::Client;
use std::{
    path::Path,
};
use tokio::{fs::File, io::AsyncWriteExt};
use tokio_util::io::ReaderStream;
use tracing::{error, info};
use uuid::Uuid;

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Server is healthy", body = String)
    )
)]
pub async fn health_check() -> &'static str {
    "OK"
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
pub async fn convert_media(
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
    let mut output_format = "mp3".to_string();
    let mut has_callback = false;
    let mut input_ext = "unknown".to_string();

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "file" => {
                if let Some(file_name) = field.file_name() {
                    if let Some(ext) = Path::new(file_name).extension().and_then(|s| s.to_str()) {
                        input_ext = ext.to_string();
                    }
                }
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

    if let Some(url_str) = &url_opt {
        if let Ok(parsed_url) = reqwest::Url::parse(url_str) {
            if let Some(path_seg) = parsed_url.path_segments() {
                if let Some(last_seg) = path_seg.last() {
                    if let Some(ext) = Path::new(last_seg).extension().and_then(|s| s.to_str()) {
                        input_ext = ext.to_string();
                    }
                }
            }
        }
    }

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

    let job_type_str = format!("Convert (Sync: {} -> {})", input_ext, output_format);

    update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Processing", 0, None);

    let ffmpeg_res = run_ffmpeg(Path::new(&input_path), Path::new(&out_path), &output_format).await;

    let _ = tokio::fs::remove_file(&input_path).await;

    if let Err(e) = &ffmpeg_res {
        update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Failed", 0, Some(e.to_string()));
        ffmpeg_res?;
    }

    update_job_status(&state.dashboard, uuid.clone(), &job_type_str, "Success", 0, None);

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
pub async fn convert_media_async(
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
    let mut output_format = "mp3".to_string();
    let mut include_file = false;
    let mut input_ext = "unknown".to_string();

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "file" => {
                if let Some(file_name) = field.file_name() {
                    if let Some(ext) = Path::new(file_name).extension().and_then(|s| s.to_str()) {
                        input_ext = ext.to_string();
                    }
                }
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

    if let Some(url_str) = &url_opt {
        if let Ok(parsed_url) = reqwest::Url::parse(url_str) {
            if let Some(path_seg) = parsed_url.path_segments() {
                if let Some(last_seg) = path_seg.last() {
                    if let Some(ext) = Path::new(last_seg).extension().and_then(|s| s.to_str()) {
                        input_ext = ext.to_string();
                    }
                }
            }
        }
    }

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

    if let Some(mut manager) = state.redis_manager {
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
pub async fn download_file_endpoint(
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
pub async fn admin_cleanup_endpoint(
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
