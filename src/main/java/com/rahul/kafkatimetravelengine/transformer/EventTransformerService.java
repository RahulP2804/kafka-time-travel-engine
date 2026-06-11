package com.rahul.kafkatimetravelengine.transformer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms a matched ConsumerRecord into a ProducerRecord ready for the target topic.
 *
 * <p>When {@code applyTransformer=true}, the following replay metadata headers are injected:
 * <ul>
 *   <li>{@code X-Replayed-At}       — ISO-8601 timestamp when the event was replayed</li>
 *   <li>{@code X-Original-Offset}   — original Kafka partition offset of the source event</li>
 *   <li>{@code X-Original-Partition} — source partition number</li>
 *   <li>{@code X-Original-Topic}    — source topic name</li>
 *   <li>{@code X-Replay-Job-Id}     — the replay job UUID for traceability</li>
 * </ul>
 *
 * <p>When {@code applyTransformer=false}, the record is forwarded as-is with existing headers preserved.
 */
@Component
public class EventTransformerService {

    private static final Logger log = LoggerFactory.getLogger(EventTransformerService.class);

    public static final String HEADER_REPLAYED_AT        = "X-Replayed-At";
    public static final String HEADER_ORIGINAL_OFFSET    = "X-Original-Offset";
    public static final String HEADER_ORIGINAL_PARTITION = "X-Original-Partition";
    public static final String HEADER_ORIGINAL_TOPIC     = "X-Original-Topic";
    public static final String HEADER_REPLAY_JOB_ID      = "X-Replay-Job-Id";

    /**
     * Converts a source ConsumerRecord to a ProducerRecord targeting the specified topic.
     * Optionally enriches with replay metadata headers.
     */
    public ProducerRecord<String, String> transform(ConsumerRecord<String, String> source,
                                                    String targetTopic,
                                                    boolean applyTransformer,
                                                    String jobId) {
        // Copy existing headers so originals are not mutated
        List<Header> headers = new ArrayList<>();
        source.headers().forEach(headers::add);

        if (applyTransformer) {
            // Inject replay provenance headers for downstream consumers to identify replayed events
            headers.add(header(HEADER_REPLAYED_AT,        Instant.now().toString()));
            headers.add(header(HEADER_ORIGINAL_OFFSET,    String.valueOf(source.offset())));
            headers.add(header(HEADER_ORIGINAL_PARTITION, String.valueOf(source.partition())));
            headers.add(header(HEADER_ORIGINAL_TOPIC,     source.topic()));
            headers.add(header(HEADER_REPLAY_JOB_ID,      jobId));

            log.debug("Injected replay headers for key={} originalOffset={}",
                    source.key(), source.offset());
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                targetTopic,
                null,           // partition: let Kafka decide based on key
                source.key(),
                source.value()
        );

        headers.forEach(h -> record.headers().add(h));
        return record;
    }

    private RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
