package com.rahul.kafkatimetravelengine.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Thin wrapper around {@link KafkaTemplate} for publishing replay events.
 *
 * <p>Uses async send with a completion callback to avoid blocking the replay
 * engine thread while still capturing publish failures in the log.
 */
@Component
public class KafkaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a pre-built ProducerRecord to Kafka.
     * Returns the CompletableFuture so callers can optionally await or track results.
     */
    public CompletableFuture<SendResult<String, String>> publish(ProducerRecord<String, String> record) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event key={} to topic={}: {}",
                        record.key(), record.topic(), ex.getMessage(), ex);
            } else {
                log.debug("Published event key={} to topic={} partition={} offset={}",
                        record.key(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * Flushes all pending send requests. Call after batch publishing to ensure
     * all events are dispatched before the job is marked complete.
     */
    public void flush() {
        kafkaTemplate.flush();
        log.debug("KafkaTemplate flushed — all pending sends dispatched");
    }
}
