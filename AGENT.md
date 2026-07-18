# AI Agent Integration Guide — Chambapro FFmpeg API

This document provides structured instructions, schemas, and integration patterns for AI agents, code generation models, or programmatic clients to interact with the Chambapro FFmpeg API.

---

## 🔑 Authentication
If `API_KEY` is configured in the environment, all requests **must** include the following header:
```http
X-API-KEY: <your-configured-api-key>
```
*If not set, the header should be omitted.*

---

## 🔄 Execution Mode Decision Matrix

| Client Need | Optimal Endpoint | Redis Needed? | Notes |
| :--- | :--- | :--- | :--- |
| **Instant Result** (Files < 10MB) | `POST /convert` | No | Blocks until conversion completes. Streams binary back immediately. |
| **Non-blocking / Large Files** | `POST /convert-async` | Optional | Returns `202 Accepted` immediately with a UUID. Notifies client via webhook. |

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
  - `callback_url` (String, Required): Webhook URL that will receive the status update.
  - `include_file` (Boolean, Optional, Default: `false`):
    - `true`: The final webhook will send the converted file as raw binary.
    - `false`: The final webhook will send a JSON payload containing a `/download/:file_name` link.
  - `headers` (String, Optional): A raw JSON object containing custom headers to retrieve the file from `url` (e.g., `{"Authorization": "Bearer token"}`).

#### Example Request
```bash
curl -X POST http://<host>/convert-async \
  -H "X-API-KEY: secret_key" \
  -F "url=https://example.com/audio.oga" \
  -F "output_format=mp3" \
  -F "callback_url=https://your-server.com/webhook" \
  -F "include_file=false"
```

#### Immediate Response (202 Accepted)
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "enqueue": true
}
```

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

---

## 📥 File Storage & Downloads: `GET /download/:file_name`
- Retrieve converted output files using the `download_url` provided in the success webhook.
- **Retention**: Files are deleted automatically after `CLEANUP_HOURS` (default: `24`). If accessed after expiration, the endpoint returns `404 Not Found`.

---

## 🚨 Error Handling Strategy for AI Agents

1. **HTTP 400 Mismatch**: If you call `/convert` with a `callback_url` parameter, it will fail. Ensure you check for the presence of a callback before selecting the target endpoint.
2. **Download URL Expiration**: When consuming async results, fetch the binary from `download_url` immediately upon webhook receipt to avoid expiration.
3. **Queue State Inspection**: You can monitor active job lifecycle states and fetch current system logs dynamically by calling `GET /api/dashboard`.
