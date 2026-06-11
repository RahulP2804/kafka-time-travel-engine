package com.rahul.kafkatimetravelengine.filter;

import com.rahul.kafkatimetravelengine.model.FilterRule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Applies a list of FilterRules to a single ConsumerRecord.
 *
 * <p>Semantics: ALL rules must match (logical AND).
 * An empty or null rule list always passes.
 */
@Component
public class EventFilterService {

    private static final Logger log = LoggerFactory.getLogger(EventFilterService.class);

    /**
     * Returns true if the record satisfies every rule in the list.
     * Short-circuits on the first failing rule.
     */
    public boolean matches(ConsumerRecord<String, String> record, List<FilterRule> rules) {
        if (rules == null || rules.isEmpty()) {
            return true;
        }

        for (FilterRule rule : rules) {
            if (!matchesSingleRule(record, rule)) {
                log.debug("Record key={} failed rule: target={} op={} value={}",
                        record.key(), rule.target(), rule.operator(), rule.value());
                return false;
            }
        }
        return true;
    }

    private boolean matchesSingleRule(ConsumerRecord<String, String> record, FilterRule rule) {
        String candidate = resolveCandidate(record, rule);

        if (candidate == null) {
            // Field not present — treat as no-match
            return false;
        }

        return switch (rule.operator()) {
            case EQUALS      -> candidate.equals(rule.value());
            case CONTAINS    -> candidate.contains(rule.value());
            case STARTS_WITH -> candidate.startsWith(rule.value());
            case REGEX       -> compileAndMatch(rule.value(), candidate);
        };
    }

    private String resolveCandidate(ConsumerRecord<String, String> record, FilterRule rule) {
        return switch (rule.target()) {
            case KEY -> record.key();
            case HEADER -> {
                if (rule.headerKey() == null || rule.headerKey().isBlank()) {
                    log.warn("FilterRule with target=HEADER has no headerKey set — skipping rule");
                    yield null;
                }
                Header header = record.headers().lastHeader(rule.headerKey());
                yield header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
            }
        };
    }

    private boolean compileAndMatch(String pattern, String candidate) {
        try {
            return Pattern.compile(pattern).matcher(candidate).matches();
        } catch (PatternSyntaxException e) {
            log.error("Invalid regex pattern '{}': {}", pattern, e.getMessage());
            return false;
        }
    }
}
