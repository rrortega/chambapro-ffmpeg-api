use axum::{
    body::Body,
    extract::Multipart,
    http::{header, StatusCode, HeaderMap},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
    Router,
};
use reqwest::Client;
use std::{collections::HashMap, net::SocketAddr, path::Path};
use tempfile::NamedTempFile;
use tokio::{fs::File, io::AsyncWriteExt, process::Command};
use tokio_util::io::ReaderStream;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    http_client: Client,
    api_key: Option<String>,
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

    let state = AppState {
        http_client: Client::new(),
        api_key,
    };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/convert", post(convert_media))
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
    axum::extract::State(state): axum::extract::State<AppState>,
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
    let mut input_file_opt: Option<NamedTempFile> = None;
    let mut url_opt: Option<String> = None;
    let mut headers_opt: Option<String> = None;
    let mut callback_url_opt: Option<String> = None;
    let mut output_format = "mp3".to_string(); // default

    while let Some(mut field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "file" => {
                let temp_file = NamedTempFile::new()?;
                let file_path = temp_file.path().to_owned();
                let mut f = tokio::fs::File::create(&file_path).await?;
                
                while let Some(chunk) = field.chunk().await.unwrap_or(None) {
                    f.write_all(&chunk).await?;
                }
                f.flush().await?;
                input_file_opt = Some(temp_file);
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
            "output_format" => {
                if let Ok(fmt) = field.text().await {
                    output_format = fmt;
                }
            }
            _ => {}
        }
    }

    // Determine input source
    let input_temp_file = if let Some(file) = input_file_opt {
        file
    } else if let Some(url) = url_opt {
        download_file(&state.http_client, &url, headers_opt.as_deref()).await?
    } else {
        return Ok((StatusCode::BAD_REQUEST, "Missing 'file' or 'url' field").into_response());
    };

    // Handle asynchronous callback (webhook) if callback_url is provided
    if let Some(callback_url) = callback_url_opt {
        let client = state.http_client.clone();
        tokio::spawn(async move {
            info!("Enqueued background conversion task targeting webhook: {}", callback_url);
            if let Err(e) = process_and_callback(client, input_temp_file, callback_url, output_format).await {
                error!("Background processing and callback webhook failed: {:?}", e);
            }
        });

        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "enqueue": true })),
        ).into_response());
    }

    let output_temp_file = NamedTempFile::new()?;
    let out_path = output_temp_file.path().to_owned();

    // Call ffmpeg
    run_ffmpeg(input_temp_file.path(), &out_path, &output_format).await?;

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

    // Spawn a task to delete the file AFTER the stream completes
    // By keeping the output_temp_file in a spawn block, we keep it alive until the body is processed (though in a full app we'd tie its lifetime to the Body more tightly).
    // Actually on Unix, we can just drop it, the fd stays open!
    // But to be clean we just rely on Unix behavior here.
    
    Ok(response)
}

async fn download_file(client: &Client, url: &str, headers_json: Option<&str>) -> anyhow::Result<NamedTempFile> {
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

    let temp_file = NamedTempFile::new()?;
    let mut f = tokio::fs::File::create(temp_file.path()).await?;

    while let Some(chunk) = res.chunk().await? {
        f.write_all(&chunk).await?;
    }
    f.flush().await?;

    Ok(temp_file)
}

async fn process_and_callback(
    client: Client,
    input_temp_file: NamedTempFile,
    callback_url: String,
    output_format: String,
) -> anyhow::Result<()> {
    let output_temp_file = NamedTempFile::new()?;
    let out_path = output_temp_file.path().to_owned();

    // Call ffmpeg
    run_ffmpeg(input_temp_file.path(), &out_path, &output_format).await?;

    // Stream the output file directly into the multipart request body (most optimized binary transfer)
    let file = File::open(&out_path).await?;
    let stream = ReaderStream::new(file);
    let body = reqwest::Body::wrap_stream(stream);

    let file_part = reqwest::multipart::Part::stream(body)
        .file_name(format!("output.{}", output_format))
        .mime_str(match output_format.as_str() {
            "mp3" => "audio/mpeg",
            "mp4" => "video/mp4",
            "wav" => "audio/wav",
            "ogg" => "audio/ogg",
            "webm" => "video/webm",
            _ => "application/octet-stream",
        })?;

    let form = reqwest::multipart::Form::new().part("file", file_part);

    info!("Sending converted file via webhook to {}", callback_url);
    let res = client.post(&callback_url).multipart(form).send().await?;
    
    let status = res.status();
    if !status.is_success() {
        let err_body = res.text().await.unwrap_or_else(|_| "Unknown error".to_string());
        anyhow::bail!("Webhook callback to {} failed (status {}): {}", callback_url, status, err_body);
    }
    
    info!("Webhook callback to {} successfully completed", callback_url);
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
        let res = download_file(&client, "http://invalid-url-12345.com", None).await;
        assert!(res.is_err());
    }
}
