package com.rahul.kafkatimetravelengine.model;

import java.time.Instant;
import java.util.List;

/**
 * Response returned immediately after a replay job is submitted.
 * Poll /api/v1/replay/{jobId}/audit for final status and counts.
 */
public record ReplayResponse(
        String jobId,
        String status,
        String sourceTopic,
        String targetTopic,
        boolean dryRun,
        Instant submittedAt,
        String message,

        /** Populated only for dry-run jobs — sample of matched event keys. */
        List<String> dryRunSampleKeys
) {

    public static ReplayResponse accepted(String jobId, ReplayRequest request) {
        return new ReplayResponse(
                jobId,
                "ACCEPTED",
                request.sourceTopic(),
                request.targetTopic(),
                request.dryRun(),
                Instant.now(),
                "Replay job accepted. Check /api/v1/replay/" + jobId + "/audit for status.",
                null
        );
    }

    public static ReplayResponse dryRunCompleted(String jobId, ReplayRequest request,
                                                  int matchedCount, List<String> sampleKeys) {
        return new ReplayResponse(
                jobId,
                "DRY_RUN_COMPLETED",
                request.sourceTopic(),
                request.targetTopic(),
                true,
                Instant.now(),
                "Dry run complete. " + matchedCount + " events matched. No events published.",
                sampleKeys
        );
    }
}
