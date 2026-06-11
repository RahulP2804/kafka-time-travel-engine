package com.rahul.kafkatimetravelengine.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rahul.kafkatimetravelengine.model.AuditEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Persists and retrieves {@link AuditEntry} records in Redis.
 *
 * <p>Each entry is keyed as {@code replay:audit:{jobId}} and expires
 * after the configured TTL (default 30 days).
 */
@Component
public class AuditStore {

    private static final Logger log = LoggerFactory.getLogger(AuditStore.class);
    private static final String KEY_PREFIX = "replay:audit:";

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.audit.ttl-hours:720}")
    private long ttlHours;

    public AuditStore(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /** Persists an audit entry, overwriting any existing entry for the same jobId. */
    public void save(AuditEntry entry) {
        String key = buildKey(entry.jobId());
        try {
            String json = objectMapper.writeValueAsString(entry);
            redisTemplate.opsForValue().set(key, json, ttlHours, TimeUnit.HOURS);
            log.debug("Saved audit entry jobId={} status={}", entry.jobId(), entry.status());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize audit entry jobId={}: {}", entry.jobId(), e.getMessage());
        }
    }

    /** Retrieves an audit entry by jobId. Returns empty if not found or expired. */
    public Optional<AuditEntry> findById(String jobId) {
        String key = buildKey(jobId);
        String json = redisTemplate.opsForValue().get(key);

        if (json == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(objectMapper.readValue(json, AuditEntry.class));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize audit entry jobId={}: {}", jobId, e.getMessage());
            return Optional.empty();
        }
    }

    private String buildKey(String jobId) {
        return KEY_PREFIX + jobId;
    }
}
