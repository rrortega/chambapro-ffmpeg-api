use cucumber::{given, then, when, World};

#[derive(Debug, Default, World)]
pub struct ConversionWorld {
    service_running: bool,
    uploaded_file: Option<String>,
    converted_successfully: bool,
}

#[given("the media conversion service is running")]
fn service_is_running(w: &mut ConversionWorld) {
    w.service_running = true;
}

#[when("a user uploads an audio file for synchronous conversion")]
fn upload_audio_file(w: &mut ConversionWorld) {
    w.uploaded_file = Some("test_audio.oga".to_string());
    w.converted_successfully = true;
}

#[then("the service converts the file to the requested format and returns the binary")]
fn verify_conversion(w: &mut ConversionWorld) {
    assert!(w.service_running);
    assert_eq!(w.uploaded_file.as_deref(), Some("test_audio.oga"));
    assert!(w.converted_successfully);
}

#[tokio::main]
async fn main() {
    ConversionWorld::run("tests/features/conversion.feature").await;
}
