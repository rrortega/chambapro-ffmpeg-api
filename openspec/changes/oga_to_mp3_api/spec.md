# Specification: Audio/Video Conversion API

## 1. Overview
This API provides a fast, reliable service for converting audio and video files. The initial primary use case is converting `.oga` (Ogg Vorbis/Opus) audio files to `.mp3` format. It is designed to be highly concurrent, robust, and capable of handling varying input methods.

## 2. Endpoints

### 2.1 Health Check
*   **Method**: `GET`
*   **Path**: `/health`
*   **Description**: Returns the health status of the API.
*   **Response**: `200 OK` with a simple JSON or text payload indicating the service is running.

### 2.2 Convert Media
*   **Method**: `POST`
*   **Path**: `/convert`
*   **Content-Type**: `multipart/form-data`
*   **Description**: Converts an input media file to a specified format and returns the converted file.

#### 2.2.1 Request Parameters (Multipart Form Fields)
The endpoint accepts *either* a direct file upload or a remote URL to download the file from.

*   `file` (Optional): The binary data of the file to be converted.
*   `url` (Optional): A remote URL from which to download the source file.
*   `headers` (Optional): A JSON-encoded string containing custom HTTP headers to use when downloading the remote `url`. This is useful for authenticated downloads.
    *   *Example*: `{"Authorization": "Bearer token123", "User-Agent": "CustomApp/1.0"}`
*   `output_format` (Optional): The desired output format extension (e.g., `mp3`). Defaults to `mp3` if not provided.

*Note: At least one of `file` or `url` must be provided. If both are provided, the API may either return an error or prioritize one over the other (prioritizing `file` is recommended).*

#### 2.2.2 Responses
*   **Success (200 OK)**:
    *   **Content-Type**: The MIME type of the converted file (e.g., `audio/mpeg` for MP3).
    *   **Body**: A continuous binary stream of the converted media.
*   **Client Error (400 Bad Request)**:
    *   Missing both `file` and `url`.
    *   Invalid `headers` JSON.
    *   Unsupported `output_format`.
*   **Server Error (500 Internal Server Error)**:
    *   Failed to download remote URL.
    *   Conversion process (ffmpeg) failed.
    *   File system or streaming errors.

## 3. Core Behavior & Requirements
1.  **Temporary Storage**: Any input files downloaded or uploaded MUST be stored in temporary locations that are guaranteed to be cleaned up after the request completes, fails, or is aborted by the client.
2.  **Streaming**: The output from the conversion process should be streamed back to the client as efficiently as possible to reduce memory overhead.
3.  **Authentication Forwarding**: When fetching from a remote URL, the API must securely apply the provided custom headers.
4.  **Error Handling**: Detailed logs should be generated for conversion failures. The client should receive clean, standardized error responses without exposing internal system paths.
