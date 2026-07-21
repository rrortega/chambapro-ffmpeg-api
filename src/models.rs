use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct DashboardJob {
    pub uuid: String,
    pub job_type: String,
    pub status: String, // "Enqueued", "Processing", "Success", "Failed"
    pub retries: u32,
    pub error: Option<String>,
    pub timestamp: String,
}

#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct RequestMetric {
    pub timestamp: String, // RFC3339 string
    pub duration_ms: u64,
    pub endpoint: String,
    pub status: u16,
}

pub struct DashboardState {
    pub jobs: Vec<DashboardJob>,
    pub logs: Vec<String>,
    pub metrics: Vec<RequestMetric>,
}

#[derive(Clone)]
pub struct SharedDashboardState(pub Arc<RwLock<DashboardState>>);

#[derive(Clone)]
pub struct AppState {
    pub http_client: Client,
    pub api_key: Option<String>,
    pub redis_manager: Option<redis::aio::ConnectionManager>,
    pub storage_dir: String,
    pub host_url: String,
    pub max_retries: u32,
    pub cleanup_hours: u64,
    pub dashboard: SharedDashboardState,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobType {
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
pub struct Job {
    pub id: String,
    pub job_type: JobType,
}

#[derive(Clone)]
pub struct DashboardLogWriter {
    pub state: Arc<RwLock<DashboardState>>,
    pub storage_dir: String,
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

        // Also write to log files on disk
        let logs_dir = format!("{}/dashboard/logs", self.storage_dir);
        let _ = std::fs::create_dir_all(&logs_dir);
        let now = chrono::Local::now();
        let file_path = format!("{}/logs_{}.txt", logs_dir, now.format("%Y-%m-%d"));
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
        {
            let _ = file.write_all(buf);
        }

        std::io::stdout().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

pub struct AppError(pub anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("Error: {:?}", self.0);
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

pub async fn track_metrics(
    axum::extract::State(state): axum::extract::State<AppState>,
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

        if let Ok(mut db_state) = state.dashboard.0.write() {
            db_state.metrics.push(new_metric.clone());
            if db_state.metrics.len() > 2000 {
                db_state.metrics.remove(0);
            }
        }

        let storage_dir = state.storage_dir.clone();
        tokio::spawn(async move {
            let metrics_dir = format!("{}/dashboard/metrics", storage_dir);
            let path = format!("{}/{}.json", metrics_dir, uuid::Uuid::new_v4());
            if let Ok(content) = serde_json::to_string(&new_metric) {
                let _ = tokio::fs::write(path, content).await;
            }
        });
    }

    response
}

pub fn update_job_status(
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

        if let Some(job) = job_updated {
            let storage_dir = std::env::var("STORAGE_DIR").unwrap_or_else(|_| "./storage".to_string());
            tokio::spawn(async move {
                let jobs_dir = format!("{}/dashboard/jobs", storage_dir);
                let path = format!("{}/{}.json", jobs_dir, job.uuid);
                if let Ok(content) = serde_json::to_string(&job) {
                    let _ = tokio::fs::write(path, content).await;
                }
            });
        }
    }
}
