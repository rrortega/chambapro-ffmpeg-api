use crate::models::{AppError, AppState, DashboardJob, RequestMetric, SharedDashboardState, update_job_status};
use axum::{
    extract::State,
    response::Html,
    Json,
};
use chrono::TimeZone;
use tracing::{error, info};

pub async fn dashboard_page() -> Html<String> {
    match tokio::fs::read_to_string("templates/dashboard.html").await {
        Ok(content) => Html(content),
        Err(_) => {
            Html(include_str!("../templates/dashboard.html").to_string())
        }
    }
}

pub async fn dashboard_api(
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

async fn perform_dashboard_disk_cleanup(storage_dir: &str) -> anyhow::Result<()> {
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(30 * 24 * 3600);
    let mut cleaned_count = 0;

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

pub async fn perform_directory_cleanup(
    storage_dir: &str,
    cleanup_hours: u64,
    dashboard: &SharedDashboardState,
) -> anyhow::Result<()> {
    let mut dir = tokio::fs::read_dir(storage_dir).await?;
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(cleanup_hours * 3600);
    let mut cleaned_count = 0;

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

pub async fn load_dashboard_from_disk(storage_dir: &str) -> crate::models::DashboardState {
    let mut jobs = Vec::new();
    let mut metrics = Vec::new();

    let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
    let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
    let _ = tokio::fs::create_dir_all(&jobs_dir).await;
    let _ = tokio::fs::create_dir_all(&metrics_dir).await;

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

    crate::models::DashboardState {
        jobs,
        logs: Vec::new(),
        metrics,
    }
}
