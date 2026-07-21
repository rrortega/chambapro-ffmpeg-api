use crate::models::{Job, JobType, SharedDashboardState, update_job_status};
use reqwest::Client;
use std::{
    collections::HashMap,
    path::Path,
};
use tokio::{fs::File, io::AsyncWriteExt, process::Command};
use tokio_util::io::ReaderStream;
use tracing::{error, info, warn};
use uuid::Uuid;

pub async fn run_queue_workers(
    mut manager: redis::aio::ConnectionManager,
    client: Client,
    storage_dir: String,
    host_url: String,
    max_retries: u32,
    cleanup_hours: u64,
    dashboard: SharedDashboardState,
) -> anyhow::Result<()> {
    info!("Starting Chambapro queue workers...");

    let mut manager_delayed = manager.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

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

    loop {
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
            let task_start = std::time::Instant::now();
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

                    let elapsed = task_start.elapsed();
                    if let Err(e) = conversion_res {
                        let next_retry = retry_count + 1;
                        warn!("Conversion failed for UUID {} (attempt {}/{}): {:?}. Total task duration: {:?}", uuid, next_retry, max_retries, e, elapsed);
                        
                        if next_retry >= max_retries {
                            update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Failed", max_retries, Some(e.to_string()));
                            let _ = tokio::fs::remove_file(&input_path).await;
                            if !callback_url.is_empty() {
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
                            }
                        } else {
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
                        update_job_status(&dashboard_clone, uuid.clone(), &job_type_str, "Success", retry_count, None);
                        info!("Redis job conversion succeeded for UUID {}. Total task duration: {:?}", uuid, elapsed);
                        let _ = tokio::fs::remove_file(&input_path).await;
                        
                        if !callback_url.is_empty() {
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
                        }

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

pub async fn enqueue_job(manager: &mut redis::aio::ConnectionManager, job: Job) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(&job)?;
    let _: () = redis::Cmd::lpush("chambapro:queue", serialized)
        .query_async(manager)
        .await?;
    Ok(())
}

pub async fn enqueue_delayed_job(
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

pub async fn download_file(
    client: &Client,
    url: &str,
    headers_json: Option<&str>,
    dest_path: &str,
) -> anyhow::Result<()> {
    info!("Starting download of remote file from URL: {}", url);
    let start_time = std::time::Instant::now();

    let mut req = client.get(url);
    if let Some(h) = headers_json {
        let headers: HashMap<String, String> = serde_json::from_str(h)?;
        for (k, v) in headers {
            req = req.header(k, v);
        }
    }

    let mut res = req.send().await?;
    if !res.status().is_success() {
        error!("Failed to download file from {}, HTTP status: {}", url, res.status());
        anyhow::bail!("Failed to download file, status: {}", res.status());
    }

    if let Some(len) = res.content_length() {
        info!("Remote file size (Content-Length): {} bytes ({:.2} KB) from URL: {}", len, len as f64 / 1024.0, url);
    } else {
        info!("Remote file size is unknown (no Content-Length header) from URL: {}", url);
    }

    let mut f = File::create(dest_path).await?;
    while let Some(chunk) = res.chunk().await? {
        f.write_all(&chunk).await?;
    }
    f.flush().await?;

    let elapsed = start_time.elapsed();
    let meta = tokio::fs::metadata(dest_path).await?;
    info!("Successfully downloaded remote file. Destination: {:?}. Size on disk: {} bytes ({:.2} KB). Time elapsed: {:?}", dest_path, meta.len(), meta.len() as f64 / 1024.0, elapsed);

    Ok(())
}

#[derive(serde::Deserialize, Debug)]
struct ProbeOutput {
    streams: Option<Vec<ProbeStream>>,
    format: Option<ProbeFormat>,
}

#[derive(serde::Deserialize, Debug)]
struct ProbeStream {
    codec_type: Option<String>,
    sample_rate: Option<String>,
    channels: Option<i32>,
}

#[derive(serde::Deserialize, Debug)]
struct ProbeFormat {
    probe_score: Option<i32>,
}

async fn validate_file_metadata(path: &Path, require_audio: bool) -> anyhow::Result<()> {
    if !path.exists() {
        anyhow::bail!("File does not exist");
    }

    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=probe_score:stream=channels,sample_rate,codec_type")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffprobe failed: {}", stderr.trim());
    }

    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let probe: ProbeOutput = serde_json::from_str(&stdout_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse ffprobe JSON output: {}", e))?;

    // Check probe score to protect against fake/empty files
    if let Some(format) = probe.format {
        if let Some(score) = format.probe_score {
            if score < 20 {
                anyhow::bail!("File media container matching confidence too low (probe_score: {})", score);
            }
        } else {
            anyhow::bail!("Missing probe score in ffprobe metadata");
        }
    } else {
        anyhow::bail!("Missing format section in ffprobe metadata");
    }

    // Check streams
    if let Some(streams) = probe.streams {
        if streams.is_empty() {
            anyhow::bail!("No media streams found in file");
        }

        let mut has_valid_stream = false;
        for s in streams {
            let matches_type = if require_audio {
                s.codec_type.as_deref() == Some("audio")
            } else {
                s.codec_type.as_deref() == Some("audio") || s.codec_type.as_deref() == Some("video")
            };

            if matches_type {
                let channels = s.channels.unwrap_or(0);
                let sample_rate = s.sample_rate.as_deref()
                    .and_then(|sr| sr.parse::<i32>().ok())
                    .unwrap_or(0);

                if channels > 0 && sample_rate > 0 {
                    has_valid_stream = true;
                    break;
                }
            }
        }

        if !has_valid_stream {
            if require_audio {
                anyhow::bail!("No valid audio stream with channels and sample rate found");
            } else {
                anyhow::bail!("No valid audio or video stream found");
            }
        }
    } else {
        anyhow::bail!("Missing streams section in ffprobe metadata");
    }

    Ok(())
}

async fn validate_input_file(path: &Path) -> anyhow::Result<()> {
    info!("Running input integrity check (ffprobe) on file: {:?}", path);
    if let Err(e) = validate_file_metadata(path, false).await {
        error!("Input file integrity check failed: {}", e);
        return Err(e);
    }
    info!("Input file integrity check passed.");
    Ok(())
}

async fn validate_output_audio_file(path: &Path) -> anyhow::Result<()> {
    info!("Running output audio validation check on file: {:?}", path);
    if let Err(e) = validate_file_metadata(path, true).await {
        error!("Output audio validation check failed: {}", e);
        return Err(e);
    }
    info!("Output audio validation check passed.");
    Ok(())
}

pub async fn run_ffmpeg(input_path: &Path, output_path: &Path, format: &str) -> anyhow::Result<()> {
    info!("Running ffmpeg from {:?} to {:?} format {}", input_path, output_path, format);

    // 1. Validate input file before conversion
    if let Err(e) = validate_input_file(input_path).await {
        warn!("Input validation failed for {:?}: {}", input_path, e);
        return Err(e);
    }

    // 2. Run conversion
    let output = Command::new("ffmpeg")
        .arg("-y")
        .arg("-hide_banner")
        .arg("-i")
        .arg(input_path)
        .arg("-f")
        .arg(format)
        .arg(output_path)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let trimmed = stderr.trim();
        error!("ffmpeg failed: {}", trimmed);
        let _ = tokio::fs::remove_file(output_path).await;
        anyhow::bail!("ffmpeg conversion failed: {}", trimmed);
    }

    // 3. Validate output file to ensure it's a valid audio file
    if let Err(e) = validate_output_audio_file(output_path).await {
        warn!("Output validation failed for {:?}: {}", output_path, e);
        let _ = tokio::fs::remove_file(output_path).await;
        return Err(e);
    }

    info!("ffmpeg conversion successful");
    Ok(())
}

pub async fn send_webhook_with_file(
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

pub async fn send_simple_webhook_success(
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

pub async fn send_simple_webhook_error(
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
