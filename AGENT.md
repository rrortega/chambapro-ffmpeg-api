# AI Agent Integration & Developer Guide — Chambapro FFmpeg API

This document provides structured instructions, schemas, environment configurations, and workflow patterns for AI agents, code generation models, or programmatic clients to install, run, implement, and extend the Chambapro FFmpeg API.

---

## 🔑 Authentication
If `API_KEY` is configured in the environment, all requests **must** include the following header:
```http
X-API-KEY: <your-configured-api-key>
```
*If not set, the header should be omitted.*

---

## 🔄 Execution Mode Decision Matrix

| Client Need | Optimal Endpoint | Webhook Needed? | Notes |
| :--- | :--- | :--- | :--- |
| **Instant Result** (Files < 10MB) | `POST /convert` | Forbidden | Blocks until conversion completes. Streams binary back immediately. |
| **Asynchronous (Webhooks)** | `POST /convert-async` | Yes (supply `callback_url`) | Returns `202 Accepted` immediately with a UUID. Notifies client via webhook. |
| **Asynchronous (Polling)** | `POST /convert-async` | No (omit `callback_url`) | Returns `202 Accepted`. Client polls `GET /status/:uuid` to retrieve link. |

---

## 🛠️ API Specifications for AI Agents

### 1. Synchronous Conversion: `POST /convert`
*Blocks and returns the raw file.*
- **Format**: `multipart/form-data`
- **Fields**:
  - `file` (File, Optional): The media binary file to convert.
  - `url` (String, Optional): The remote URL to download and convert instead.
  - `output_format` (String, Optional, Default: `mp3`): Target format (e.g. `mp3`, `mp4`, `wav`, `ogg`, `webm`).
- **Constraints**: **Do not** send `callback_url` to this endpoint; it will return `400 Bad Request`.

#### Example Request
```bash
curl -X POST http://<host>/convert \
  -H "X-API-KEY: secret_key" \
  -F "file=@audio.oga" \
  -F "output_format=mp3" \
  --output output.mp3
```

---

### 2. Asynchronous Queue: `POST /convert-async`
*Responds instantly and executes the job in the background.*
- **Format**: `multipart/form-data`
- **Fields**:
  - `file` (File, Optional): Media binary file.
  - `url` (String, Optional): Remote URL to download and convert.
  - `output_format` (String, Optional, Default: `mp3`): Target format.
  - `callback_url` (String, Optional): Webhook URL that will receive the status update. If omitted, poll `/status/:uuid` instead.
  - `include_file` (Boolean, Optional, Default: `false`):
    - `true`: The final webhook will send the converted file as raw binary (only applicable if `callback_url` is provided).
    - `false`: The final webhook will send a JSON payload containing a `/download/:file_name` link.
  - `headers` (String, Optional): A raw JSON object containing custom headers to retrieve the file from `url` (e.g., `{"Authorization": "Bearer token"}`).

#### Example Request (Polling Style)
```bash
curl -X POST http://<host>/convert-async \
  -H "X-API-KEY: secret_key" \
  -F "url=https://example.com/audio.oga" \
  -F "output_format=mp3"
```

#### Immediate Response (202 Accepted)
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "enqueue": true
}
```

---

### 3. Check Job Status: `GET /status/:uuid`
*Queries the real-time processing state of an asynchronous job.*
- **Parameter**: `uuid` (Path parameter, string)
- **Response (200 OK)**:
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "job_type": "Convert (Redis: oga -> mp3)",
  "status": "Success",
  "retries": 0,
  "error": null,
  "timestamp": "2026-07-18 22:45:10",
  "download_url": "http://<host>/download/7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d.mp3"
}
```
*Note: `download_url` is only present when status is `"Success"`.*

---

## 🚀 AI Agent Installation & Development Instructions

If you are an AI assistant tasked with installing, testing, or building this project, follow this exact recipe:

### 1. Install System Dependencies
Ensure FFmpeg is installed and accessible in the system path:
- **macOS**: `brew install ffmpeg`
- **Debian/Ubuntu**: `sudo apt-get update && sudo apt-get install -y ffmpeg`
- **Alpine**: `apk add --no-cache ffmpeg`
- **Redis (Optional for queuing)**: Ensure a Redis instance is running locally or provide a remote `REDIS_URL`. If Redis is offline, the API automatically falls back to internal memory-based background task runner threads.

### 2. Configure Environment Variables
Copy `.env.example` to `.env` or inject variables programmatically:
```bash
PORT=80
API_KEY=secret_key                 # Optional API Key access protection
REDIS_URL=redis://127.0.0.1:6379   # Optional Redis URL. Leave blank for memory mode.
PUBLIC_URL=http://localhost        # Used to build download URLs
MAX_RETRIES=3                      # Number of FFmpeg worker conversion attempts
CLEANUP_HOURS=24                   # Temp files retention limit
STORAGE_DIR=./storage              # Local storage path for uploads
```

### 3. Building and Running local server
Use Rust Cargo CLI to manage compilation:
- **Build**: `cargo build --release`
- **Run dev server**: `cargo run`

### 4. Running the Test Suite
This project implements both unit tests and Gherkin BDD Cucumber scenarios:
- **Run all tests (Unit + Cucumber BDD)**:
  ```bash
  cargo test
  ```
- **Structure of Cucumber BDD**:
  - Gherkin feature definition: `tests/features/conversion.feature`
  - Step implementations: `tests/cucumber_tests.rs`

### 5. Codebase Modularity Blueprint
When extending or adding endpoints:
- **REST Endpoints**: Append to `src/routes.rs`. Mark endpoints with `#[utoipa::path(...)]` for OpenAPI registration.
- **OpenAPI Doc**: Add the route path to the `ApiDoc` list macro in `src/main.rs`.
- **Background workers / Webhooks**: Extend queue tasks in `src/worker.rs`.
- **UI Customizations**: Edit `templates/dashboard.html` directly (hot-reloadable at runtime).

---

## 🪝 Webhook Payload Schemas

When background processing completes, the API sends a `POST` request to your `callback_url`.

### Payload 1: Success (when `include_file` is `false`)
Sent as `application/json`:
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "success": true,
  "message": "File converted successfully. Available for download for 24 hours.",
  "download_url": "http://<host>/download/7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d.mp3"
}
```

### Payload 2: Success (when `include_file` is `true`)
Sent as `multipart/form-data`:
- `uuid` (String): The job UUID.
- `file` (File): The raw binary of the converted file.

### Payload 3: Failure (Always JSON)
Sent as `application/json`:
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "success": false,
  "error": "ffmpeg conversion failed: Invalid audio packet size"
}
```
