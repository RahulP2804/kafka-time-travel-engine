package com.rahul.kafkatimetravelengine.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

/**
 * Request to replay events from a Dead-Letter Queue topic back to the original topic.
 * The same filter and transform pipeline applies as a regular replay job.
 */
public record DlqReplayRequest(

        @NotBlank(message = "dlqTopic must not be blank")
        String dlqTopic,

        @NotBlank(message = "targetTopic must not be blank")
        String targetTopic,

        /** Optional filters to replay only a subset of DLQ events. */
        @Valid
        List<FilterRule> filterRules,

        /** When true, adds replay metadata headers to re-published events. */
        boolean applyTransformer,

        /** When true, logs matched events without re-publishing them. */
        boolean dryRun,

        String requestedBy
) {
    public DlqReplayRequest {
        if (requestedBy == null || requestedBy.isBlank()) {
            requestedBy = "anonymous";
        }
    }
}
