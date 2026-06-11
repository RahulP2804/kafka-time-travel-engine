package com.rahul.kafkatimetravelengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe binding for {@code app.*} properties in application.yml.
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        ReplayProperties replay,
        AuditProperties audit
) {

    public record ReplayProperties(
            long pollTimeoutMs,
            int maxEventsPerReplay,
            RateLimitProperties rateLimit
    ) {}

    public record RateLimitProperties(
            long capacity,
            long refillTokens,
            long refillPeriodSeconds
    ) {}

    public record AuditProperties(
            long ttlHours
    ) {}
}
