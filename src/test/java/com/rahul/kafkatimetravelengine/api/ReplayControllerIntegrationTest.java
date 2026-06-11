package com.rahul.kafkatimetravelengine.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rahul.kafkatimetravelengine.model.ReplayRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class ReplayControllerIntegrationTest {

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
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void postReplay_validRequest_returns202WithJobId() throws Exception {
        ReplayRequest request = new ReplayRequest(
                "source-topic", "target-topic",
                Instant.now().minusSeconds(3600),
                Instant.now(),
                null, false, true, "test-user"
        );

        mockMvc.perform(post("/api/v1/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.dryRun").value(true));
    }

    @Test
    void postReplay_missingSourceTopic_returns400() throws Exception {
        String invalidJson = """
                {
                  "targetTopic": "target",
                  "startTime": "2024-01-01T00:00:00Z",
                  "endTime":   "2024-01-01T01:00:00Z"
                }
                """;

        mockMvc.perform(post("/api/v1/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"));
    }

    @Test
    void getAudit_unknownJobId_returns404() throws Exception {
        mockMvc.perform(get("/api/v1/replay/non-existent-job-id/audit"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("JOB_NOT_FOUND"));
    }

    @Test
    void postReplay_endTimeBeforeStartTime_returns400() throws Exception {
        String invalidJson = """
                {
                  "sourceTopic": "src",
                  "targetTopic": "tgt",
                  "startTime": "2024-06-01T12:00:00Z",
                  "endTime":   "2024-06-01T10:00:00Z"
                }
                """;

        mockMvc.perform(post("/api/v1/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                .andExpect(status().isBadRequest());
    }
}
