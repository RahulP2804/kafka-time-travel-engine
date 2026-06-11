package com.rahul.kafkatimetravelengine.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;

/**
 * Inbound request to initiate a time-windowed Kafka replay job.
 * All filter rules are optional — omitting them replays all events in the window.
 */
public record ReplayRequest(

        @NotBlank(message = "sourceTopic must not be blank")
        String sourceTopic,

        @NotBlank(message = "targetTopic must not be blank")
        String targetTopic,

        @NotNull(message = "startTime must not be null")
        Instant startTime,

        @NotNull(message = "endTime must not be null")
        Instant endTime,

        /**
         * Optional filter rules. Events must satisfy ALL rules (AND semantics).
         * Leave null or empty to replay everything in the window.
         */
        @Valid
        List<FilterRule> filterRules,

        /**
         * When true, matched events are transformed before being published
         * to the target topic (adds replay metadata headers).
         */
        boolean applyTransformer,

        /**
         * When true, matched events are logged but NOT published.
         * Useful for auditing what would be replayed without side effects.
         */
        boolean dryRun,

        /**
         * Free-text label identifying who or what triggered this replay.
         * Used in the audit trail.
         */
        String requestedBy
) {
    public ReplayRequest {
        if (startTime != null && endTime != null && !endTime.isAfter(startTime)) {
            throw new IllegalArgumentException("endTime must be after startTime");
        }
        if (requestedBy == null || requestedBy.isBlank()) {
            requestedBy = "anonymous";
        }
    }
}
