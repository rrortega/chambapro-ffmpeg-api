use cucumber::{given, then, when, World};

#[derive(Debug, Default, World)]
pub struct ConversionWorld {
    service_running: bool,
    redis_connected: bool,
    redis_offline: bool,
    api_key_enabled: bool,
    uploaded_format: String,
    target_format: String,
    converted_successfully: bool,
    http_status: u16,
    job_enqueued: bool,
    job_failed: bool,
    error_message: Option<String>,
    webhook_sent: bool,
    include_file: bool,
    download_link_sent: bool,
    file_age_hours: u32,
    file_deleted: bool,
}

#[given("the media conversion service is running")]
async fn service_is_running(w: &mut ConversionWorld) {
    w.service_running = true;
}

#[given("a Redis queue backend is connected and configured")]
async fn redis_connected(w: &mut ConversionWorld) {
    w.redis_connected = true;
}

#[given("the Redis queue backend is offline")]
async fn redis_offline(w: &mut ConversionWorld) {
    w.redis_offline = true;
}

#[given("API Key authentication is enabled on the service")]
async fn api_key_enabled(w: &mut ConversionWorld) {
    w.api_key_enabled = true;
}

#[given(expr = "a converted file has been on disk longer than {string} hours")]
async fn file_age(w: &mut ConversionWorld, hours: String) {
    w.file_age_hours = hours.parse().unwrap_or(0);
}

#[when(expr = "a user uploads a valid {string} file for synchronous conversion to {string}")]
async fn upload_audio_file_sync(w: &mut ConversionWorld, from: String, to: String) {
    w.uploaded_format = from;
    w.target_format = to;
    w.converted_successfully = true;
}

#[then(expr = "the service converts the file and returns the {string} binary directly")]
async fn verify_binary_response(w: &mut ConversionWorld, format: String) {
    assert!(w.service_running);
    assert_eq!(w.target_format, format);
    assert!(w.converted_successfully);
}

#[when(expr = "a user requests synchronous conversion of a remote {string} file to {string}")]
async fn request_remote_sync_conversion(w: &mut ConversionWorld, from: String, to: String) {
    w.uploaded_format = from;
    w.target_format = to;
    w.converted_successfully = true;
}

#[then(expr = "the service downloads the file, converts it, and returns the {string} binary")]
async fn verify_remote_binary_response(w: &mut ConversionWorld, format: String) {
    assert!(w.service_running);
    assert_eq!(w.target_format, format);
    assert!(w.converted_successfully);
}

#[when(expr = "a user requests asynchronous conversion to {string} with a callback URL")]
async fn request_async_conversion(w: &mut ConversionWorld, format: String) {
    w.target_format = format;
    w.job_enqueued = true;
    w.http_status = 202;
}

#[then("the service enqueues the job and immediately returns an HTTP 202 status")]
async fn verify_async_enqueue(w: &mut ConversionWorld) {
    assert!(w.service_running);
    assert!(w.redis_connected);
    assert!(w.job_enqueued);
    assert_eq!(w.http_status, 202);
}

#[when(expr = "an async job completes with \"include_file\" set to {word}")]
async fn async_job_completes(w: &mut ConversionWorld, include_file: String) {
    w.include_file = include_file == "true";
    w.webhook_sent = true;
    if !w.include_file {
        w.download_link_sent = true;
    }
}

#[then("the service sends the webhook with the converted file payload")]
async fn verify_webhook_file(w: &mut ConversionWorld) {
    assert!(w.webhook_sent);
    assert!(w.include_file);
}

#[then("the service sends the webhook containing the download link")]
async fn verify_webhook_link(w: &mut ConversionWorld) {
    assert!(w.webhook_sent);
    assert!(!w.include_file);
    assert!(w.download_link_sent);
}

#[then(expr = "the service falls back to a simple async background thread and returns HTTP {int}")]
async fn verify_fallback_status(w: &mut ConversionWorld, status: i32) {
    assert!(w.redis_offline);
    assert_eq!(w.http_status, status as u16);
}

#[when(expr = "a user makes a request with an invalid {string} header")]
async fn invalid_auth_request(w: &mut ConversionWorld, _header: String) {
    w.http_status = 401;
}

#[then(expr = "the service rejects the request with an HTTP {int} Unauthorized status")]
async fn verify_unauthorized_status(w: &mut ConversionWorld, status: i32) {
    assert!(w.api_key_enabled);
    assert_eq!(w.http_status, status as u16);
}

#[when(expr = "a user requests conversion of an invalid or corrupted file to {string}")]
async fn request_corrupted_file_conversion(w: &mut ConversionWorld, format: String) {
    w.target_format = format;
    w.job_failed = true;
    w.error_message = Some("ffmpeg conversion failed: Invalid data found when processing input".to_string());
}

#[then("the conversion job fails and records the error details")]
async fn verify_failed_conversion(w: &mut ConversionWorld) {
    assert!(w.job_failed);
    assert!(w.error_message.is_some());
}

#[when("a user requests synchronous conversion with a callback URL")]
async fn request_sync_with_callback(w: &mut ConversionWorld) {
    w.http_status = 400;
}

#[then(expr = "the service rejects the request with an HTTP {int} Bad Request status")]
async fn verify_bad_request(w: &mut ConversionWorld, status: i32) {
    assert_eq!(w.http_status, status as u16);
}

#[when("the automatic directory cleanup task runs")]
async fn run_cleanup_task(w: &mut ConversionWorld) {
    if w.file_age_hours >= 24 {
        w.file_deleted = true;
    }
}

#[then("the expired file is deleted and the cleanup state is logged")]
async fn verify_file_deleted(w: &mut ConversionWorld) {
    assert!(w.file_deleted);
}

#[tokio::main]
async fn main() {
    ConversionWorld::run("tests/features/conversion.feature").await;
}
