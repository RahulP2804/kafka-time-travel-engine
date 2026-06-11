package com.rahul.kafkatimetravelengine.transformer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class EventTransformerServiceTest {

    private EventTransformerService transformerService;

    @BeforeEach
    void setUp() {
        transformerService = new EventTransformerService();
    }

    @Test
    void withTransformerEnabled_injectsReplayHeaders() {
        ConsumerRecord<String, String> source = new ConsumerRecord<>("source-topic", 2, 42L, "key-1", "value-1");
        String jobId = "test-job-id";

        ProducerRecord<String, String> result = transformerService.transform(source, "target-topic", true, jobId);

        assertThat(result.topic()).isEqualTo("target-topic");
        assertThat(result.key()).isEqualTo("key-1");
        assertThat(result.value()).isEqualTo("value-1");

        assertHeaderValue(result, EventTransformerService.HEADER_ORIGINAL_OFFSET,    "42");
        assertHeaderValue(result, EventTransformerService.HEADER_ORIGINAL_PARTITION, "2");
        assertHeaderValue(result, EventTransformerService.HEADER_ORIGINAL_TOPIC,     "source-topic");
        assertHeaderValue(result, EventTransformerService.HEADER_REPLAY_JOB_ID,      "test-job-id");
        // X-Replayed-At is dynamic — just assert presence
        assertThat(result.headers().lastHeader(EventTransformerService.HEADER_REPLAYED_AT)).isNotNull();
    }

    @Test
    void withTransformerDisabled_noReplayHeadersInjected() {
        ConsumerRecord<String, String> source = new ConsumerRecord<>("source-topic", 0, 10L, "key-2", "value-2");

        ProducerRecord<String, String> result = transformerService.transform(source, "target-topic", false, "job-id");

        assertThat(result.headers().lastHeader(EventTransformerService.HEADER_REPLAYED_AT)).isNull();
        assertThat(result.headers().lastHeader(EventTransformerService.HEADER_ORIGINAL_OFFSET)).isNull();
        assertThat(result.headers().lastHeader(EventTransformerService.HEADER_REPLAY_JOB_ID)).isNull();
    }

    @Test
    void existingSourceHeaders_arePreservedAfterTransform() {
        ConsumerRecord<String, String> source = new ConsumerRecord<>("src", 0, 1L, "k", "v");
        source.headers().add("X-Correlation-Id", "corr-123".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> result = transformerService.transform(source, "tgt", true, "job");

        assertHeaderValue(result, "X-Correlation-Id", "corr-123");
    }

    @Test
    void nullKeyAndValue_handledGracefully() {
        ConsumerRecord<String, String> source = new ConsumerRecord<>("src", 0, 1L, null, null);
        ProducerRecord<String, String> result = transformerService.transform(source, "tgt", true, "job");

        assertThat(result.key()).isNull();
        assertThat(result.value()).isNull();
    }

    // ---- helpers ----

    private void assertHeaderValue(ProducerRecord<?, ?> record, String headerKey, String expectedValue) {
        Header header = record.headers().lastHeader(headerKey);
        assertThat(header).as("Expected header '%s' to be present", headerKey).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(expectedValue);
    }
}
