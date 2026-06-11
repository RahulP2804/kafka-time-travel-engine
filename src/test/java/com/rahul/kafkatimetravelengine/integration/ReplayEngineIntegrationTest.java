package com.rahul.kafkatimetravelengine.integration;

import com.rahul.kafkatimetravelengine.engine.ReplayEngine;
import com.rahul.kafkatimetravelengine.model.AuditEntry;
import com.rahul.kafkatimetravelengine.model.FilterRule;
import com.rahul.kafkatimetravelengine.model.ReplayRequest;
import com.rahul.kafkatimetravelengine.storage.AuditStore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplayEngineIntegrationTest {

    static final String SOURCE_TOPIC = "integration-source";
    static final String TARGET_TOPIC = "integration-target";

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @SuppressWarnings("resource")
    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> "");
    }

    @Autowired
    private ReplayEngine replayEngine;

    @Autowired
    private AuditStore auditStore;

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void happyPath_replayEventsInWindow_publishesToTargetTopic() throws Exception {
        Instant before = Instant.now();
        seedEvents(SOURCE_TOPIC, List.of("order-1", "order-2", "order-3"), 3);
        Thread.sleep(100); // small gap so endTime is clearly after the seeded messages
        Instant after = Instant.now();

        ReplayRequest request = new ReplayRequest(
                SOURCE_TOPIC, TARGET_TOPIC,
                before, after,
                null,  // no filters
                true,  // apply transformer
                false, // not dry-run
                "integration-test"
        );

        String jobId = replayEngine.submitReplay(request);

        await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            AuditEntry audit = auditStore.findById(jobId).orElseThrow();
            assertThat(audit.status()).isEqualTo("COMPLETED");
            assertThat(audit.eventsMatched()).isGreaterThanOrEqualTo(3);
            assertThat(audit.eventsPublished()).isGreaterThanOrEqualTo(3);
        });
    }

    @Test
    void emptyWindow_noEventsMatched_completesCleanly() {
        // Time window in the distant past — no events exist there
        Instant pastStart = Instant.parse("2000-01-01T00:00:00Z");
        Instant pastEnd   = Instant.parse("2000-01-01T00:01:00Z");

        ReplayRequest request = new ReplayRequest(
                SOURCE_TOPIC, TARGET_TOPIC,
                pastStart, pastEnd,
                null, false, false, "integration-test"
        );

        String jobId = replayEngine.submitReplay(request);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            AuditEntry audit = auditStore.findById(jobId).orElseThrow();
            assertThat(audit.status()).isEqualTo("COMPLETED");
            assertThat(audit.eventsMatched()).isEqualTo(0);
            assertThat(audit.eventsPublished()).isEqualTo(0);
        });
    }

    @Test
    void dryRun_logsEventsWithoutPublishing() throws Exception {
        Instant before = Instant.now();
        seedEvents(SOURCE_TOPIC, List.of("dry-run-key-1", "dry-run-key-2"), 2);
        Thread.sleep(100);
        Instant after = Instant.now();

        ReplayRequest request = new ReplayRequest(
                SOURCE_TOPIC, TARGET_TOPIC,
                before, after,
                null, false,
                true, // DRY RUN
                "integration-test"
        );

        String jobId = replayEngine.submitReplay(request);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            AuditEntry audit = auditStore.findById(jobId).orElseThrow();
            assertThat(audit.status()).isEqualTo("DRY_RUN_COMPLETED");
            assertThat(audit.eventsMatched()).isGreaterThanOrEqualTo(2);
            // Dry-run must NOT publish
            assertThat(audit.eventsPublished()).isEqualTo(0);
        });
    }

    @Test
    void filterByKeyPrefix_onlyMatchingEventsReplayed() throws Exception {
        String filteredTopic = "filter-test-source";
        Instant before = Instant.now();
        seedEvents(filteredTopic, List.of("order-A", "order-B", "payment-X", "payment-Y"), 4);
        Thread.sleep(100);
        Instant after = Instant.now();

        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null,
                FilterRule.FilterOperator.STARTS_WITH, "order-");

        ReplayRequest request = new ReplayRequest(
                filteredTopic, TARGET_TOPIC,
                before, after,
                List.of(rule),
                false, false,
                "integration-test"
        );

        String jobId = replayEngine.submitReplay(request);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            AuditEntry audit = auditStore.findById(jobId).orElseThrow();
            assertThat(audit.status()).isEqualTo("COMPLETED");
            // Only the 2 "order-" events should match
            assertThat(audit.eventsMatched()).isEqualTo(2);
            assertThat(audit.eventsPublished()).isEqualTo(2);
        });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void seedEvents(String topic, List<String> keys, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String key : keys) {
                producer.send(new ProducerRecord<>(topic, key, "payload-for-" + key)).get();
            }
            producer.flush();
        }
    }
}
