Feature: Media Conversion Service

  Background:
    Given the media conversion service is running

  Scenario: Synchronous media conversion
    When a user uploads a valid audio file for synchronous conversion to "mp3"
    Then the service converts the file and returns the binary directly

  Scenario: Asynchronous conversion with queue enqueuing
    Given a Redis queue backend is connected and configured
    When a user requests asynchronous conversion to "wav" with a callback URL
    Then the service enqueues the job and immediately returns an HTTP 202 status

  Scenario: Failed conversion due to missing input
    When a user requests conversion of a non-existent file
    Then the conversion job fails and records the error details
