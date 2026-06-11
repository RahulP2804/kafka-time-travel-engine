package com.rahul.kafkatimetravelengine.api;

import com.rahul.kafkatimetravelengine.engine.ReplayEngine;
import com.rahul.kafkatimetravelengine.exception.JobNotFoundException;
import com.rahul.kafkatimetravelengine.exception.RateLimitExceededException;
import com.rahul.kafkatimetravelengine.model.AuditEntry;
import com.rahul.kafkatimetravelengine.model.DlqReplayRequest;
import com.rahul.kafkatimetravelengine.model.ReplayRequest;
import com.rahul.kafkatimetravelengine.model.ReplayResponse;
import com.rahul.kafkatimetravelengine.storage.AuditStore;
import io.github.bucket4j.Bucket;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for submitting and monitoring Kafka replay jobs.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code POST /api/v1/replay}         — submit a time-windowed replay job</li>
 *   <li>{@code POST /api/v1/replay/dlq}     — replay events from a DLQ topic</li>
 *   <li>{@code GET  /api/v1/replay/{jobId}/audit} — retrieve the audit record for a job</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/replay")
public class ReplayController {

    private static final Logger log = LoggerFactory.getLogger(ReplayController.class);

    private final ReplayEngine replayEngine;
    private final AuditStore auditStore;
    private final Bucket rateLimiter;

    public ReplayController(ReplayEngine replayEngine, AuditStore auditStore, Bucket rateLimiter) {
        this.replayEngine = replayEngine;
        this.auditStore   = auditStore;
        this.rateLimiter  = rateLimiter;
    }

    /**
     * Submits a time-windowed Kafka replay job.
     *
     * <p>The job runs asynchronously. The response contains the jobId which can be
     * used to poll the audit endpoint for status and counts.
     */
    @PostMapping
    public ResponseEntity<ReplayResponse> submitReplay(@Valid @RequestBody ReplayRequest request) {
        enforceRateLimit();

        log.info("Replay request received: {} → {} | window [{}, {}] | dryRun={}",
                request.sourceTopic(), request.targetTopic(),
                request.startTime(), request.endTime(), request.dryRun());

        String jobId = replayEngine.submitReplay(request);

        ReplayResponse response = ReplayResponse.accepted(jobId, request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * Submits a DLQ replay job.
     * Reads all events from the specified DLQ topic and replays them to the target topic.
     */
    @PostMapping("/dlq")
    public ResponseEntity<ReplayResponse> submitDlqReplay(@Valid @RequestBody DlqReplayRequest request) {
        enforceRateLimit();

        log.info("DLQ replay request received: {} → {} | dryRun={}",
                request.dlqTopic(), request.targetTopic(), request.dryRun());

        String jobId = replayEngine.submitDlqReplay(request);

        // Build a minimal synthetic ReplayRequest for the response constructor
        ReplayResponse response = new ReplayResponse(
                jobId,
                "ACCEPTED",
                request.dlqTopic(),
                request.targetTopic(),
                request.dryRun(),
                java.time.Instant.now(),
                "DLQ replay job accepted. Check /api/v1/replay/" + jobId + "/audit for status.",
                null
        );
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * Returns the audit record for a submitted replay job.
     * Status values: RUNNING | COMPLETED | DRY_RUN_COMPLETED | FAILED
     */
    @GetMapping("/{jobId}/audit")
    public ResponseEntity<AuditEntry> getAudit(@PathVariable String jobId) {
        AuditEntry audit = auditStore.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        return ResponseEntity.ok(audit);
    }

    private void enforceRateLimit() {
        if (!rateLimiter.tryConsume(1)) {
            throw new RateLimitExceededException(
                    "Replay rate limit exceeded. Too many replay jobs submitted in a short window. " +
                    "Please wait before submitting another replay job.");
        }
    }
}
