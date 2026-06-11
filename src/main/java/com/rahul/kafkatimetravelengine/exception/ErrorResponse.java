package com.rahul.kafkatimetravelengine.exception;

import java.time.Instant;

/**
 * Standard error response shape returned by the global exception handler.
 */
public record ErrorResponse(
        String error,
        String message,
        Instant timestamp
) {
    public static ErrorResponse of(String error, String message) {
        return new ErrorResponse(error, message, Instant.now());
    }
}
