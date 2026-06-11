package com.rahul.kafkatimetravelengine.model;

import java.time.Instant;
import java.util.List;

/**
 * Immutable audit record for a single replay job.
 * Serialized as JSON and stored in Redis with a configurable TTL.
 *
 * <p>Status lifecycle: RUNNING → COMPLETED | FAILED | DRY_RUN_COMPLETED
 */
public record AuditEntry(
        String jobId,
        String requestedBy,
        String sourceTopic,
        String targetTopic,
        Instant startTime,
        Instant endTime,
        List<FilterRule> filterRules,
        boolean applyTransformer,
        boolean dryRun,
        int eventsMatched,
        int eventsPublished,
        String status,
        String failureReason,
        Instant jobStartedAt,
        Instant jobCompletedAt
) {

    /** Creates an initial RUNNING audit entry at job start. */
    public static AuditEntry running(String jobId, ReplayRequest request) {
        return new AuditEntry(
                jobId,
                request.requestedBy(),
                request.sourceTopic(),
                request.targetTopic(),
                request.startTime(),
                request.endTime(),
                request.filterRules(),
                request.applyTransformer(),
                request.dryRun(),
                0,
                0,
                "RUNNING",
                null,
                Instant.now(),
                null
        );
    }

    /** Returns a completed copy with final counts. */
    public AuditEntry completed(int matched, int published) {
        return new AuditEntry(
                jobId, requestedBy, sourceTopic, targetTopic,
                startTime, endTime, filterRules, applyTransformer, dryRun,
                matched, published,
                dryRun ? "DRY_RUN_COMPLETED" : "COMPLETED",
                null,
                jobStartedAt,
                Instant.now()
        );
    }

    /** Returns a failed copy with reason. */
    public AuditEntry failed(String reason) {
        return new AuditEntry(
                jobId, requestedBy, sourceTopic, targetTopic,
                startTime, endTime, filterRules, applyTransformer, dryRun,
                eventsMatched, eventsPublished,
                "FAILED",
                reason,
                jobStartedAt,
                Instant.now()
        );
    }
}
