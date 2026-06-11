package com.rahul.kafkatimetravelengine.config;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Bucket4j rate limiter configuration for replay endpoints.
 *
 * <p>A single shared bucket is used across all replay requests.
 * Tune {@code app.replay.rate-limit.*} in application.yml to control capacity.
 *
 * <p>Default: 10 tokens capacity, refilling 5 tokens every 60 seconds.
 * This prevents runaway replay jobs from overwhelming Kafka brokers.
 */
@Configuration
public class RateLimiterConfig {

    private final AppProperties appProperties;

    public RateLimiterConfig(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    @Bean
    public Bucket replayRateLimiter() {
        AppProperties.RateLimitProperties rl = appProperties.replay().rateLimit();

        Bandwidth limit = Bandwidth.classic(
                rl.capacity(),
                Refill.greedy(rl.refillTokens(), Duration.ofSeconds(rl.refillPeriodSeconds()))
        );

        return Bucket.builder()
                .addLimit(limit)
                .build();
    }
}
