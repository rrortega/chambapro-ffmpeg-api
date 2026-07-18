# Implementation Tasks

This document outlines the step-by-step dependency-ordered tasks required to build the Audio/Video Conversion API.

## Phase 1: Setup and Infrastructure
- [x] **Task 1.1**: Initialize a new Rust project (`cargo init`).
- [x] **Task 1.2**: Add core dependencies to `Cargo.toml`: `axum`, `tokio`, `serde`, `serde_json`, `reqwest`, `tempfile`, `tokio-util`, `multipart` (or axum's multipart extractor), `tracing`, `tracing-subscriber`.
- [x] **Task 1.3**: Create a highly optimized multi-stage `Dockerfile` using `cargo-chef` for dependency caching and a Debian/Alpine runtime layer with `ffmpeg` installed.
- [x] **Task 1.4**: Setup `tracing` for structured logging in `main.rs`.

## Phase 2: Core Routing and Basic Handlers
- [x] **Task 2.1**: Implement the `GET /health` endpoint.
- [x] **Task 2.2**: Setup the Axum application router and bind it to a local port (e.g., 0.0.0.0:8080).
- [x] **Task 2.3**: Define the skeleton for the `POST /convert` endpoint and register it in the router.

## Phase 3: Media Ingestion
- [x] **Task 3.1**: Implement a module to parse multipart form data, extracting `file`, `url`, `headers`, and `output_format`.
- [x] **Task 3.2**: Implement a function to handle direct file uploads, writing the incoming multipart stream safely to a `tempfile::NamedTempFile`.
- [x] **Task 3.3**: Implement a function using `reqwest` to download a file from a URL to a `NamedTempFile`, explicitly parsing and appending the custom headers provided in the JSON string.

## Phase 4: FFmpeg Integration
- [x] **Task 4.1**: Create a module to wrap `tokio::process::Command` calls to `ffmpeg`.
- [x] **Task 4.2**: Implement a function that takes an input file path, an output file path, and a target format, and executes `ffmpeg -y -i <in> -f <format> <out>`.
- [x] **Task 4.3**: Add error handling to capture and log `ffmpeg` stderr output upon failure.

## Phase 5: Assembly and Egress
- [x] **Task 5.1**: Integrate the ingestion and FFmpeg modules into the `POST /convert` handler.
- [x] **Task 5.2**: Implement the response logic to open the generated output `NamedTempFile` and stream it back to the client using `axum::body::Body::from_stream`.
- [x] **Task 5.3**: Ensure that temp file scoping is strictly maintained so that early returns (errors) and successful streams both result in automatic file cleanup.

## Phase 6: Testing and Refinement
- [x] **Task 6.1**: Write a unit test or integration test for the remote URL download logic.
- [x] **Task 6.2**: Write a test executing the `ffmpeg` wrapper on a dummy file.
- [x] **Task 6.3**: Refine HTTP error codes and JSON error messages returned to the client.
