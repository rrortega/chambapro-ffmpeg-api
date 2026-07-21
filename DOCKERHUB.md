# Chambapro FFmpeg API 🚀

High-performance, ultra-lightweight Rust-based API for audio and video conversion using FFmpeg. Designed for high concurrency, reliability, and scale. Supports Redis task queues with automatic failover to local memory queues, OpenTelemetry tracing, and clean REST interfaces.

> 🔗 **For interactive architectural flowcharts, full performance benchmarks, and separate bilingual files, visit the [GitHub Repository](https://github.com/rrortega).**

---

## ✨ Features / Características
- **Dual Async Processing**: Runs high-throughput task queues via Redis, with automatic graceful fallback to local background threads if Redis is offline.
- **Pre & Post Conversion Validation**: Built-in verification utilizing `ffprobe` to ensure uploaded/downloaded files are valid, and output files contain a decodable audio stream.
- **Log Rotation & Disk Protection**: Appends execution logs to disk under `storage/dashboard/logs/` with an automatic 48-hour cleanup sweep to protect storage.
- **Bilingual OpenAPI Docs**: Full interactive Swagger UI available out of the box at `/docs`.
- **Telemetry & Monitoring**: Native OpenTelemetry integration exporting to standard OTLP backends (e.g., Datadog, Honeycomb, New Relic) and an interactive live dashboard with a real-time log search and highlight tool.
- **Robust Failure Recovery**: Conversion retries with exponential backoff, temporary file retention limits, and automated 30-day metrics cleanup.

---

## 🇺🇸 Quick Start (English)

### 1. Run with Docker
```bash
docker run -d -p 80:80 \
  -v $(pwd)/storage:/app/storage \
  -e REDIS_URL=redis://your-redis-host:6379 \
  -e PUBLIC_URL=http://your-server-ip \
  rrortega/chambapro-ffmpeg-api:latest
```

### 2. API Endpoints
- `GET /` - Redirects to Swagger UI documentation at `/docs`.
- `GET /docs` - Serves the interactive Swagger UI.
- `GET /health` - Returns `OK` for health checks.
- `POST /convert` - **Synchronous** conversion. Returns the output file directly in the response.
- `POST /convert-async` - **Asynchronous** conversion. Callback URL is optional. Returns `202 Accepted` with a job `uuid`.
- `GET /status/:uuid` - Returns real-time job status and output `download_url` once completed.
- `GET /download/:file_name` - Downloads converted files from the storage mount.
- `GET /dashboard` - Interactive web dashboard showing real-time metrics and live container stdout logs.
- `POST /admin/cleanup` - Manually triggers cleanup of temporary files in storage.

### 3. Usage Examples
**Synchronous Conversion:**
```bash
curl -X POST http://localhost/convert \
  -F "file=@input.oga" \
  -F "output_format=mp3" \
  --output output.mp3
```

**Asynchronous Conversion (Polling style):**
```bash
curl -X POST http://localhost/convert-async \
  -F "file=@input.wav" \
  -F "output_format=mp3"
```
Response:
```json
{
  "uuid": "7a94dfbd-5b0c-4464-9b2f-3b2d6a5c2f9d",
  "enqueue": true
}
```

---

## 🇪🇸 Inicio Rápido (Español)

### 1. Correr con Docker
```bash
docker run -d -p 80:80 \
  -v $(pwd)/storage:/app/storage \
  -e REDIS_URL=redis://tu-servidor-redis:6379 \
  -e PUBLIC_URL=http://ip-de-tu-servidor \
  rrortega/chambapro-ffmpeg-api:latest
```

### 2. Endpoints de la API
- `GET /` - Redirige a la interfaz de Swagger UI en `/docs`.
- `GET /docs` - Sirve la documentación interactiva de Swagger UI.
- `GET /health` - Retorna `OK` para chequeos de salud.
- `POST /convert` - Conversión **síncrona**. Retorna el archivo en la respuesta.
- `POST /convert-async` - Conversión **asíncrona** (con `callback_url` opcional). Retorna `202 Accepted` con el UUID para consulta.
- `GET /status/:uuid` - Consulta en tiempo real del estado y link de descarga del trabajo.
- `GET /download/:file_name` - Descarga de archivos convertidos del volumen persistente.
- `GET /dashboard` - Sirve un panel interactivo en tiempo real para visualizar colas, estadísticas y logs.
- `POST /admin/cleanup` - Desencadena manualmente la depuración de archivos antiguos.

### 3. Ejemplos de Uso
**Conversión Síncrona:**
```bash
curl -X POST http://localhost/convert \
  -F "file=@input.oga" \
  -F "output_format=mp3" \
  --output output.mp3
```

**Conversión Asíncrona con Webhook:**
```bash
curl -X POST http://localhost/convert-async \
  -F "url=https://ejemplo.com/audio.oga" \
  -F "output_format=mp3" \
  -F "callback_url=https://tu-webhook.com/callback"
```

---

## ⚙️ Configuration / Configuración

| Variable | Description / Descripción | Default / Por Defecto |
| :--- | :--- | :--- |
| `PORT` | Listening port / Puerto de escucha | `80` |
| `API_KEY` | (Optional) Key for X-API-KEY header / API Key de acceso | - |
| `REDIS_URL` | (Optional) Redis connection URL / Conexión a Redis | - |
| `MAX_RETRIES` | Max retries for failed jobs / Reintentos de conversión | `3` |
| `CLEANUP_HOURS` | Temp files lifetime / Horas antes de limpiar archivos | `24` |
| `STORAGE_DIR` | Mount directory / Directorio de almacenamiento | `./storage` |
| `PUBLIC_URL` | App public address / Dirección pública de descargas | `http://localhost` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (Optional) OpenTelemetry collector endpoint / Endpoint OTel | - |
| `TELEMETRY_API_KEY` | (Optional) Telemetry platform API Key / Token de telemetría | - |

---

## 👤 Author & Contributor / Autor y Colaborador

Designed, architected, and implemented by / Diseñado, estructurado e implementado por:
* **Rolando Rodriguez Ortega**
  * GitHub: [@rrortega](https://github.com/rrortega)
  * Email / Correo: rolymayo11@gmail.com
  * Repository / Repositorio: [rrortega/chambapro-ffmpeg-api](https://github.com/rrortega/chambapro-ffmpeg-api)
