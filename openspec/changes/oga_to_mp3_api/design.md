# Architecture & Design: Audio/Video Conversion API

## 1. Technical Stack

*   **Language**: Rust
*   **Web Framework**: `axum` - Recommended for modern, fast, and ergonomic web APIs in the Rust ecosystem.
*   **Async Runtime**: `tokio` - Powers Axum and provides asynchronous file I/O and process execution.
*   **HTTP Client**: `reqwest` - Used for downloading source files from remote URLs securely and efficiently.
*   **Temporary Files**: `tempfile` - Ensures that uploaded, downloaded, and converted files are automatically deleted from the filesystem when their handles go out of scope, preventing disk space leaks.
*   **Media Processing**: `ffmpeg` (CLI) via `tokio::process::Command` - Instead of linking complex native C libraries like `ffmpeg-next`, calling the `ffmpeg` CLI as an asynchronous subprocess is more robust, lighter to build, avoids complex header setups, and is much easier to containerize in Alpine/Debian environments.

## 2. Application Architecture

### 2.1 Request Flow
1.  **Ingestion**: An HTTP request arrives at `POST /convert`.
2.  **Input Resolution**:
    *   If `file` is provided in the multipart form, its stream is written directly to a newly created `tempfile::NamedTempFile`.
    *   If `url` is provided, `reqwest` fetches the URL (applying the parsed `headers` JSON) and streams the response body into a `NamedTempFile`.
3.  **Conversion**:
    *   A second `NamedTempFile` is created for the output.
    *   A `tokio::process::Command` spawns an `ffmpeg` subprocess.
    *   Arguments: `ffmpeg -y -i <input_temp_path> -f <output_format> <output_temp_path>`. (Alternatively, ffmpeg can read/write from pipes, but temp files often provide better compatibility for seeking depending on the format).
    *   The API awaits the completion of the subprocess.
4.  **Egress**:
    *   The output `NamedTempFile` is converted into an asynchronous stream (using `tokio::fs::File` and `tokio_util::codec::FramedRead`).
    *   The stream is returned to the Axum router, which streams it back to the client.
5.  **Cleanup**:
    *   Once the response is sent or the connection drops, the `NamedTempFile` variables are dropped, automatically triggering the OS to delete the files.

## 3. Containerization (Dockerfile)

To ensure rapid, cached builds and a small production footprint, a multi-stage Dockerfile is utilized.

*   **Stage 1: Planner (`cargo-chef`)**
    *   Base: `rust:slim` or `rust:alpine`
    *   Analyzes the project and generates a recipe file for dependencies.
*   **Stage 2: Builder (`cargo-chef`)**
    *   Builds the dependencies based on the recipe (this layer caches heavily).
    *   Copies the source code and builds the final release binary.
*   **Stage 3: Runtime**
    *   Base: `debian:bookworm-slim` (preferred for glibc compatibility with ffmpeg) or `alpine` (if fully static/musl).
    *   Installs the `ffmpeg` package via `apt-get` (or `apk`).
    *   Copies the compiled Rust binary from the Builder stage.
    *   Exposes the necessary port (e.g., `8080`).
    *   Sets the binary as the `ENTRYPOINT`.
