package com.rahul.kafkatimetravelengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Manual KafkaConsumer that reads events from a source topic within a specific
 * time window using Kafka's {@code offsetsForTimes} API.
 *
 * <p>This is intentionally NOT a Spring @KafkaListener — we need fine-grained
 * per-request consumer lifecycle control to seek to arbitrary offsets per replay job.
 *
 * <p>Strategy:
 * <ol>
 *   <li>Fetch all partitions for the topic</li>
 *   <li>Resolve start-time offset per partition using {@code offsetsForTimes}</li>
 *   <li>Seek each partition to its resolved start offset</li>
 *   <li>Poll records, stopping when record.timestamp() exceeds endTime or no new data</li>
 * </ol>
 */
@Component
public class KafkaReplayConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplayConsumer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.replay.poll-timeout-ms:5000}")
    private int pollTimeoutMs;

    @Value("${app.replay.max-events-per-replay:100000}")
    private int maxEventsPerReplay;

    /**
     * Reads all ConsumerRecords from {@code topic} whose timestamps fall within
     * [{@code startTime}, {@code endTime}).
     *
     * <p>Each call creates and closes its own KafkaConsumer to avoid shared state
     * between concurrent replay jobs.
     */
    public List<ConsumerRecord<String, String>> fetchEventsByTimeWindow(
            String topic, Instant startTime, Instant endTime) {

        Properties props = buildConsumerProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = getPartitionsForTopic(consumer, topic);

            if (partitions.isEmpty()) {
                log.warn("No partitions found for topic '{}' — nothing to replay", topic);
                return List.of();
            }

            consumer.assign(partitions);

            // Step 1: resolve the starting offset per partition for the given start timestamp
            Map<TopicPartition, Long> startOffsets = resolveStartOffsets(consumer, partitions, startTime);

            // Step 2: seek each partition to its resolved start offset (skip if no data before endTime)
            seekToStartOffsets(consumer, partitions, startOffsets);

            // Step 3: poll and collect records until we pass the end timestamp on all partitions
            return collectRecordsUntilEndTime(consumer, partitions, endTime);
        }
    }

    /** Resolves the earliest offset at or after startTime for each partition. */
    private Map<TopicPartition, Long> resolveStartOffsets(KafkaConsumer<String, String> consumer,
                                                           List<TopicPartition> partitions,
                                                           Instant startTime) {
        Map<TopicPartition, Long> timestampMap = new HashMap<>();
        partitions.forEach(tp -> timestampMap.put(tp, startTime.toEpochMilli()));

        // offsetsForTimes returns null for partitions that have no messages at or after the timestamp
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampMap);

        Map<TopicPartition, Long> startOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(tp);
            if (offsetAndTimestamp != null) {
                startOffsets.put(tp, offsetAndTimestamp.offset());
                log.debug("Partition {} start offset resolved to {} for timestamp {}",
                        tp, offsetAndTimestamp.offset(), startTime);
            } else {
                // No messages exist at or after startTime in this partition — skip it
                log.debug("Partition {} has no data at or after {} — will be skipped", tp, startTime);
            }
        }
        return startOffsets;
    }

    /** Seeks partitions with resolved offsets; pauses partitions with no data in the window. */
    private void seekToStartOffsets(KafkaConsumer<String, String> consumer,
                                    List<TopicPartition> partitions,
                                    Map<TopicPartition, Long> startOffsets) {
        List<TopicPartition> emptyPartitions = new ArrayList<>();

        for (TopicPartition tp : partitions) {
            Long offset = startOffsets.get(tp);
            if (offset != null) {
                consumer.seek(tp, offset);
            } else {
                emptyPartitions.add(tp);
            }
        }

        // Pause partitions that have no relevant data to avoid spinning on them
        if (!emptyPartitions.isEmpty()) {
            consumer.pause(emptyPartitions);
        }
    }

    /** Polls records, accumulating those within the time window, until all partitions are exhausted. */
    private List<ConsumerRecord<String, String>> collectRecordsUntilEndTime(
            KafkaConsumer<String, String> consumer,
            List<TopicPartition> partitions,
            Instant endTime) {

        List<ConsumerRecord<String, String>> results = new ArrayList<>();
        long endEpochMs = endTime.toEpochMilli();

        // Track which partitions have passed the end timestamp — stop polling them
        List<TopicPartition> exhaustedPartitions = new ArrayList<>();
        int consecutiveEmptyPolls = 0;

        while (exhaustedPartitions.size() < getActivePartitionCount(consumer, partitions)
                && results.size() < maxEventsPerReplay) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

            if (records.isEmpty()) {
                consecutiveEmptyPolls++;
                // Two consecutive empty polls means no more data in the window
                if (consecutiveEmptyPolls >= 2) {
                    log.debug("No more records to consume after {} empty polls — ending replay", consecutiveEmptyPolls);
                    break;
                }
                continue;
            }
            consecutiveEmptyPolls = 0;

            for (ConsumerRecord<String, String> record : records) {
                if (record.timestamp() >= endEpochMs) {
                    // We've gone past the window end on this partition — pause it
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (!exhaustedPartitions.contains(tp)) {
                        exhaustedPartitions.add(tp);
                        consumer.pause(List.of(tp));
                        log.debug("Partition {} exhausted at offset {} (timestamp {} >= endTime {})",
                                tp, record.offset(), record.timestamp(), endEpochMs);
                    }
                } else {
                    results.add(record);
                }
            }
        }

        if (results.size() >= maxEventsPerReplay) {
            log.warn("Replay hit maxEventsPerReplay limit ({}) — results may be truncated", maxEventsPerReplay);
        }

        log.info("Collected {} records from time window ending {}", results.size(), endTime);
        return results;
    }

    private int getActivePartitionCount(KafkaConsumer<?, ?> consumer, List<TopicPartition> all) {
        return all.size() - consumer.paused().size();
    }

    private List<TopicPartition> getPartitionsForTopic(KafkaConsumer<String, String> consumer, String topic) {
        return consumer.partitionsFor(topic)
                .stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    private Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Unique group ID per replay job to avoid interfering with application consumers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-time-travel-replay-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        // Fetch enough data per poll to avoid missing records in dense partitions
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }
}
