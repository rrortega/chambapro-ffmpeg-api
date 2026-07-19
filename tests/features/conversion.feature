Feature: Media Conversion Service

  Background:
    Given the media conversion service is running

  Scenario: 1. Synchronous conversion from local file upload
    When a user uploads a valid "oga" file for synchronous conversion to "mp3"
    Then the service converts the file and returns the "mp3" binary directly

  Scenario: 2. Synchronous conversion from remote URL download
    When a user requests synchronous conversion of a remote "wav" file to "ogg"
    Then the service downloads the file, converts it, and returns the "ogg" binary

  Scenario: 3. Asynchronous conversion with Redis enqueuing
    Given a Redis queue backend is connected and configured
    When a user requests asynchronous conversion to "wav" with a callback URL
    Then the service enqueues the job and immediately returns an HTTP 202 status

  Scenario: 4. Asynchronous conversion with webhook delivery containing binary
    Given a Redis queue backend is connected and configured
    When an async job completes with "include_file" set to true
    Then the service sends the webhook with the converted file payload

  Scenario: 5. Asynchronous conversion with webhook delivery containing download URL
    Given a Redis queue backend is connected and configured
    When an async job completes with "include_file" set to false
    Then the service sends the webhook containing the download link

  Scenario: 6. Asynchronous fallback when Redis is offline
    Given the Redis queue backend is offline
    When a user requests asynchronous conversion to "mp3" with a callback URL
    Then the service falls back to a simple async background thread and returns HTTP 202

  Scenario: 7. Unauthorized request with invalid API Key
    Given API Key authentication is enabled on the service
    When a user makes a request with an invalid "X-API-KEY" header
    Then the service rejects the request with an HTTP 401 Unauthorized status

  Scenario: 8. Failed conversion due to invalid media file format
    When a user requests conversion of an invalid or corrupted file to "mp3"
    Then the conversion job fails and records the error details

  Scenario: 9. Blocked callback URL on synchronous endpoint
    When a user requests synchronous conversion with a callback URL
    Then the service rejects the request with an HTTP 400 Bad Request status

  Scenario: 10. Automated file retention cleanup
    Given a converted file has been on disk longer than "24" hours
    When the automatic directory cleanup task runs
    Then the expired file is deleted and the cleanup state is logged

  Scenario: 11. Retrieve job status using the status endpoint
    When a user queries the status endpoint for an existing job UUID
    Then the service returns the job status details with HTTP 200 OK
