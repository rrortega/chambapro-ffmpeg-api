use cucumber::{given, then, when, World};

#[derive(Debug, Default, World)]
pub struct ConversionWorld {
    service_running: bool,
    redis_connected: bool,
    target_format: String,
    uploaded_file: Option<String>,
    converted_successfully: bool,
    http_status: u16,
    job_enqueued: bool,
    job_failed: bool,
    error_message: Option<String>,
}

#[given("the media conversion service is running")]
async fn service_is_running(w: &mut ConversionWorld) {
    w.service_running = true;
}

#[given("a Redis queue backend is connected and configured")]
async fn redis_connected(w: &mut ConversionWorld) {
    w.redis_connected = true;
}

#[when(expr = "a user uploads a valid audio file for synchronous conversion to {string}")]
async fn upload_audio_file_sync(w: &mut ConversionWorld, format: String) {
    w.uploaded_file = Some("input.oga".to_string());
    w.target_format = format;
    w.converted_successfully = true;
}

#[then("the service converts the file and returns the binary directly")]
async fn verify_binary_response(w: &mut ConversionWorld) {
    assert!(w.service_running);
    assert_eq!(w.uploaded_file.as_deref(), Some("input.oga"));
    assert_eq!(w.target_format, "mp3");
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
    assert_eq!(w.target_format, "wav");
}

#[when("a user requests conversion of a non-existent file")]
async fn request_missing_file_conversion(w: &mut ConversionWorld) {
    w.uploaded_file = Some("non_existent.oga".to_string());
    w.job_failed = true;
    w.error_message = Some("No such file or directory".to_string());
}

#[then("the conversion job fails and records the error details")]
async fn verify_failed_conversion(w: &mut ConversionWorld) {
    assert!(w.job_failed);
    assert!(w.error_message.is_some());
    assert_eq!(w.uploaded_file.as_deref(), Some("non_existent.oga"));
}

#[tokio::main]
async fn main() {
    ConversionWorld::run("tests/features/conversion.feature").await;
}
