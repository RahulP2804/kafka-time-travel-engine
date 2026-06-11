package com.rahul.kafkatimetravelengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Async execution configuration for replay jobs.
 *
 * <p>Replay jobs are CPU/IO-bound and potentially long-running, so they run
 * on a dedicated thread pool separate from the web server threads.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "replayTaskExecutor")
    public Executor replayTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Core threads always alive, handling concurrent replay jobs
        executor.setCorePoolSize(4);
        // Burst capacity for spike scenarios
        executor.setMaxPoolSize(10);
        // Queue replay jobs if all threads are busy (bound the queue to avoid OOM under load)
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("replay-engine-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }
}
