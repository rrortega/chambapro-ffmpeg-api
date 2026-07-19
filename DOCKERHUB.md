# Chambapro FFmpeg API 🚀

High-performance, ultra-lightweight Rust-based API for audio and video conversion using FFmpeg. Designed for high concurrency, reliability, and scale.

> 🔗 **For interactive flowcharts, full benchmarks, and separate bilingual files, visit the [GitHub Repository](https://github.com/rrortega/chambapro-ffmpeg-api).**

---

## 🇺🇸 Quick Start (English)

### 1. Run with Docker
```bash
docker run -d -p 80:80 \
  -e REDIS_URL=redis://your-redis-host:6379 \
  rrortega/chambapro-ffmpeg-api:latest
```

### 2. API Endpoints
- `GET /` - Redirects to Swagger UI documentation at `/docs`.
- `GET /docs` - Serves the interactive Swagger UI.
- `GET /health` - Returns `OK`.
- `POST /convert` - **Synchronous** conversion. Returns file directly.
- `POST /convert-async` - **Asynchronous** conversion. Requires a `callback_url`. Enqueues jobs in Redis (if configured) or executes in background (no Redis).
- `GET /download/:file_name` - Downloads converted files.
- `GET /dashboard` - Serves a beautiful, real-time web dashboard for queue status and stdout logs.
- `POST /admin/cleanup` - Manually triggers cleanup of old temporary files in storage.


### 3. Usage Examples
**Synchronous Conversion:**
```bash
curl -X POST http://localhost/convert \
  -F "file=@input.oga" \
  -F "output_format=mp3" \
  --output output.mp3
```

**Asynchronous Conversion via Webhook:**
```bash
curl -X POST http://localhost/convert-async \
  -F "url=https://example.com/audio.oga" \
  -F "output_format=mp3" \
  -F "callback_url=https://your-webhook.com/callback"
```

---

## 🇪🇸 Inicio Rápido (Español)

### 1. Correr con Docker
```bash
docker run -d -p 80:80 \
  -e REDIS_URL=redis://tu-servidor-redis:6379 \
  rrortega/chambapro-ffmpeg-api:latest
```

### 2. Endpoints de la API
- `GET /` - Redirige a la interfaz de Swagger UI en `/docs`.
- `GET /docs` - Sirve la documentación interactiva de Swagger UI.
- `GET /health` - Retorna `OK`.
- `POST /convert` - Conversión **síncrona**. Retorna el archivo en la respuesta.
- `POST /convert-async` - Conversión **asíncrona** (con `callback_url` opcional). Retorna `202 Accepted` con el UUID para consulta.
- `GET /status/:uuid` - Consulta en tiempo real del estado y link de descarga del trabajo.
- `GET /download/:file_name` - Descarga de archivos convertidos.
- `GET /dashboard` - Sirve un panel interactivo en tiempo real para visualizar colas y logs.
- `POST /admin/cleanup` - Desencadena manualmente la depuración de archivos antiguos.

### 3. Ejemplos de Uso
**Conversión Síncrona:**
```bash
curl -X POST http://localhost/convert \
  -F "file=@input.oga" \
  -F "output_format=mp3" \
  --output output.mp3
```

**Conversión Asíncrona vía Webhook:**
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
