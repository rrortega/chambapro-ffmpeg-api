# Chambapro FFmpeg API

A high-performance, Rust-based API for audio and video conversion using FFmpeg. Designed for reliability and speed, this service allows you to perform media transformations efficiently over HTTP.

## Architecture Overview

Built with modern Rust ecosystem tools to ensure maximum performance and safety:
- **Axum:** A fast and modular web framework.
- **Tokio:** An asynchronous runtime for scalable network applications.
- **Async Subprocess Execution:** FFmpeg is invoked asynchronously without blocking the main event loop, allowing high concurrency.
- **Auto-cleanup:** Temporary files are automatically managed and cleaned up to prevent disk space leaks.

## API Endpoints

### `GET /health`
Returns the health status of the API. Useful for load balancers and container orchestration probes.

### `POST /convert`
Converts media files. Supports direct file uploads or downloading from remote URLs.

**Parameters (Multipart Form Data):**
- `file` (optional): The media file to convert (if uploading directly).
- `url` (optional): The remote URL of the media file to download and convert.
- `output_format` (required): The desired output format extension (e.g., `mp4`, `mp3`, `webm`).
- `headers` (optional): A JSON string containing custom headers to use when downloading from a remote `url`.

---

## Examples

### 1. Convert via Direct File Upload
```bash
curl -X POST http://localhost:3000/convert \
  -F "file=@input.mkv" \
  -F "output_format=mp4" \
  --output output.mp4
```

### 2. Convert via Remote URL
```bash
curl -X POST http://localhost:3000/convert \
  -F "url=https://example.com/video.avi" \
  -F "output_format=mp4" \
  --output output.mp4
```

### 3. Convert via Remote URL with Custom Headers
```bash
curl -X POST http://localhost:3000/convert \
  -F "url=https://example.com/protected-video.avi" \
  -F "output_format=mp4" \
  -F 'headers={"Authorization": "Bearer YOUR_TOKEN"}' \
  --output output.mp4
```

## Running with Docker

This project uses an optimized multi-stage build process with `cargo-chef` to maximize dependency build caching and significantly reduce deployment times.

To build and run the Docker container:

```bash
# Build the image
docker build -t chambapro-ffmpeg-api .

# Run the container (exposing port 3000)
docker run -p 3000:3000 chambapro-ffmpeg-api
```

## Local Development

### Prerequisites
- [Rust](https://rustup.rs/) (latest stable)
- [FFmpeg](https://ffmpeg.org/download.html) installed and available in your system's `PATH`.

### Setup & Run
1. Clone the repository.
2. Run the server using `cargo`:

```bash
cargo run
```
The server will start on `http://127.0.0.1:3000` by default.

### Testing
You can run the test suite with:
```bash
cargo test
```
