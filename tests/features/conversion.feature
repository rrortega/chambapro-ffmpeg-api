Feature: Media Conversion Service

  Scenario: Synchronous media conversion
    Given the media conversion service is running
    When a user uploads an audio file for synchronous conversion
    Then the service converts the file to the requested format and returns the binary
