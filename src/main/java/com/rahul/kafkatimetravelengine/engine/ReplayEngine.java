package com.rahul.kafkatimetravelengine.engine;

import com.rahul.kafkatimetravelengine.filter.EventFilterService;
import com.rahul.kafkatimetravelengine.kafka.KafkaEventPublisher;
import com.rahul.kafkatimetravelengine.kafka.KafkaReplayConsumer;
import com.rahul.kafkatimetravelengine.model.AuditEntry;
import com.rahul.kafkatimetravelengine.model.DlqReplayRequest;
import com.rahul.kafkatimetravelengine.model.FilterRule;
import com.rahul.kafkatimetravelengine.model.ReplayRequest;
import com.rahul.kafkatimetravelengine.storage.AuditStore;
import com.rahul.kafkatimetravelengine.transformer.EventTransformerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core orchestration layer for Kafka event replay.
 *
 * <p>Execution flow:
 * <pre>
 * ReplayRequest
 *   → KafkaReplayConsumer.fetchEventsByTimeWindow()  (offsetsForTimes seek)
 *   → EventFilterService.matches()                   (key/header predicate)
 *   → EventTransformerService.transform()            (replay metadata headers)
 *   → KafkaEventPublisher.publish()                  (async send to target topic)
 *   → AuditStore.save()                              (Redis audit trail)
 * </pre>
 *
 * <p>Dry-run mode short-circuits before the publish step, logging matched events instead.
 */
@Service
public class ReplayEngine {

    private static final Logger log = LoggerFactory.getLogger(ReplayEngine.class);
    private static final int DRY_RUN_SAMPLE_SIZE = 10;

    private final KafkaReplayConsumer replayConsumer;
    private final KafkaEventPublisher eventPublisher;
    private final EventFilterService filterService;
    private final EventTransformerService transformerService;
    private final AuditStore auditStore;

    public ReplayEngine(KafkaReplayConsumer replayConsumer,
                        KafkaEventPublisher eventPublisher,
                        EventFilterService filterService,
                        EventTransformerService transformerService,
                        AuditStore auditStore) {
        this.replayConsumer    = replayConsumer;
        this.eventPublisher    = eventPublisher;
        this.filterService     = filterService;
        this.transformerService = transformerService;
        this.auditStore        = auditStore;
    }

    /**
     * Initiates a replay job and returns its job ID immediately.
     * Actual processing runs asynchronously via {@link #executeReplayAsync}.
     */
    public String submitReplay(ReplayRequest request) {
        String jobId = UUID.randomUUID().toString();

        // Persist initial RUNNING state before async execution begins
        AuditEntry initialAudit = AuditEntry.running(jobId, request);
        auditStore.save(initialAudit);

        log.info("[Job {}] Replay submitted: {} → {} | window [{}, {}] | dryRun={}",
                jobId, request.sourceTopic(), request.targetTopic(),
                request.startTime(), request.endTime(), request.dryRun());

        executeReplayAsync(jobId, request);
        return jobId;
    }

    /**
     * Async execution of the replay pipeline.
     * The @Async annotation requires a task executor bean — see AsyncConfig.
     */
    @Async("replayTaskExecutor")
    public void executeReplayAsync(String jobId, ReplayRequest request) {
        AuditEntry audit = AuditEntry.running(jobId, request);

        try {
            // Step 1: Fetch all events in the time window using offsetsForTimes
            log.info("[Job {}] Fetching events from '{}' between {} and {}",
                    jobId, request.sourceTopic(), request.startTime(), request.endTime());

            List<ConsumerRecord<String, String>> rawEvents = replayConsumer.fetchEventsByTimeWindow(
                    request.sourceTopic(), request.startTime(), request.endTime());

            log.info("[Job {}] Fetched {} raw events from source topic", jobId, rawEvents.size());

            // Step 2: Apply filter rules — include only events satisfying ALL rules
            List<ConsumerRecord<String, String>> matchedEvents = rawEvents.stream()
                    .filter(record -> filterService.matches(record, request.filterRules()))
                    .toList();

            log.info("[Job {}] {} events matched filter rules", jobId, matchedEvents.size());

            if (request.dryRun()) {
                // Dry-run: log matched events and sample keys — do NOT publish
                executeDryRun(jobId, matchedEvents);
                audit = audit.completed(matchedEvents.size(), 0);
            } else {
                // Live replay: transform and publish each matched event
                int published = publishMatchedEvents(jobId, matchedEvents, request);
                audit = audit.completed(matchedEvents.size(), published);
            }

        } catch (Exception ex) {
            log.error("[Job {}] Replay failed: {}", jobId, ex.getMessage(), ex);
            audit = audit.failed(ex.getMessage());
        } finally {
            auditStore.save(audit);
            log.info("[Job {}] Completed. Status={} matched={} published={}",
                    jobId, audit.status(), audit.eventsMatched(), audit.eventsPublished());
        }
    }

    /** Logs dry-run results with a sample of matched event keys. */
    private void executeDryRun(String jobId, List<ConsumerRecord<String, String>> matched) {
        log.info("[Job {}] DRY-RUN: {} events would be published. Sample keys (up to {}):",
                jobId, matched.size(), DRY_RUN_SAMPLE_SIZE);

        matched.stream()
                .limit(DRY_RUN_SAMPLE_SIZE)
                .forEach(r -> log.info("  [DRY-RUN] key={} partition={} offset={} timestamp={}",
                        r.key(), r.partition(), r.offset(), Instant.ofEpochMilli(r.timestamp())));
    }

    /** Transforms and publishes each matched event; returns the count of events successfully dispatched. */
    private int publishMatchedEvents(String jobId,
                                     List<ConsumerRecord<String, String>> matched,
                                     ReplayRequest request) {
        int published = 0;

        for (ConsumerRecord<String, String> record : matched) {
            // Transform adds replay metadata headers; passthrough preserves original headers only
            ProducerRecord<String, String> outbound = transformerService.transform(
                    record, request.targetTopic(), request.applyTransformer(), jobId);

            eventPublisher.publish(outbound);
            published++;
        }

        // Flush ensures all buffered sends are dispatched before we record the final count
        eventPublisher.flush();
        log.info("[Job {}] Dispatched {} events to '{}'", jobId, published, request.targetTopic());
        return published;
    }

    // -----------------------------------------------------------------------
    // DLQ Replay
    // -----------------------------------------------------------------------

    /**
     * Replays all events from a Dead-Letter Queue topic back to the target topic.
     * Reads from the beginning of the DLQ (no time window) and applies the same
     * filter and transform pipeline as a regular replay job.
     */
    public String submitDlqReplay(DlqReplayRequest request) {
        String jobId = UUID.randomUUID().toString();

        // Construct a synthetic ReplayRequest spanning all time to read the full DLQ
        ReplayRequest syntheticRequest = new ReplayRequest(
                request.dlqTopic(),
                request.targetTopic(),
                Instant.EPOCH,                // start from the beginning
                Instant.now().plusSeconds(1), // end just after now to capture all current messages
                request.filterRules(),
                request.applyTransformer(),
                request.dryRun(),
                request.requestedBy()
        );

        AuditEntry initialAudit = AuditEntry.running(jobId, syntheticRequest);
        auditStore.save(initialAudit);

        log.info("[Job {}] DLQ Replay submitted: {} → {} | dryRun={}",
                jobId, request.dlqTopic(), request.targetTopic(), request.dryRun());

        executeReplayAsync(jobId, syntheticRequest);
        return jobId;
    }

    /** Returns dry-run sample keys for the response payload (first N matched keys). */
    public List<String> extractDryRunSampleKeys(List<ConsumerRecord<String, String>> matched) {
        List<String> keys = new ArrayList<>();
        matched.stream()
                .limit(DRY_RUN_SAMPLE_SIZE)
                .forEach(r -> keys.add(r.key()));
        return keys;
    }
}
