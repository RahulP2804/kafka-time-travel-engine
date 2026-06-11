package com.rahul.kafkatimetravelengine.filter;

import com.rahul.kafkatimetravelengine.model.FilterRule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventFilterServiceTest {

    private EventFilterService filterService;

    @BeforeEach
    void setUp() {
        filterService = new EventFilterService();
    }

    @Test
    void emptyRuleList_alwaysMatches() {
        ConsumerRecord<String, String> record = record("any-key", "any-value");
        assertThat(filterService.matches(record, List.of())).isTrue();
    }

    @Test
    void nullRuleList_alwaysMatches() {
        ConsumerRecord<String, String> record = record("any-key", "any-value");
        assertThat(filterService.matches(record, null)).isTrue();
    }

    @Test
    void keyEquals_matchesExactKey() {
        ConsumerRecord<String, String> record = record("order-123", "payload");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null, FilterRule.FilterOperator.EQUALS, "order-123");

        assertThat(filterService.matches(record, List.of(rule))).isTrue();
    }

    @Test
    void keyEquals_doesNotMatchDifferentKey() {
        ConsumerRecord<String, String> record = record("order-456", "payload");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null, FilterRule.FilterOperator.EQUALS, "order-123");

        assertThat(filterService.matches(record, List.of(rule))).isFalse();
    }

    @Test
    void keyContains_matchesSubstring() {
        ConsumerRecord<String, String> record = record("payment-txn-9876", "payload");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null, FilterRule.FilterOperator.CONTAINS, "txn");

        assertThat(filterService.matches(record, List.of(rule))).isTrue();
    }

    @Test
    void keyStartsWith_matchesPrefix() {
        ConsumerRecord<String, String> record = record("order-123", "payload");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null, FilterRule.FilterOperator.STARTS_WITH, "order-");

        assertThat(filterService.matches(record, List.of(rule))).isTrue();
    }

    @Test
    void keyRegex_matchesPattern() {
        ConsumerRecord<String, String> record = record("order-12345", "payload");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.KEY, null, FilterRule.FilterOperator.REGEX, "order-\\d+");

        assertThat(filterService.matches(record, List.of(rule))).isTrue();
    }

    @Test
    void headerEquals_matchesHeaderValue() {
        ConsumerRecord<String, String> record = recordWithHeader("region", "UAE");
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.HEADER, "region", FilterRule.FilterOperator.EQUALS, "UAE");

        assertThat(filterService.matches(record, List.of(rule))).isTrue();
    }

    @Test
    void headerMissing_doesNotMatch() {
        ConsumerRecord<String, String> record = record("key", "value"); // no headers
        FilterRule rule = new FilterRule(FilterRule.FilterTarget.HEADER, "region", FilterRule.FilterOperator.EQUALS, "UAE");

        assertThat(filterService.matches(record, List.of(rule))).isFalse();
    }

    @Test
    void multipleRules_allMustMatch_andSemantics() {
        ConsumerRecord<String, String> record = recordWithHeader("region", "UAE");
        // Key matches but header does not — overall should fail
        FilterRule keyRule    = new FilterRule(FilterRule.FilterTarget.KEY,    null,     FilterRule.FilterOperator.STARTS_WITH, "order-");
        FilterRule headerRule = new FilterRule(FilterRule.FilterTarget.HEADER, "region", FilterRule.FilterOperator.EQUALS,      "SG");

        ConsumerRecord<String, String> r = new ConsumerRecord<>("topic", 0, 0L, "order-99", "payload");
        r.headers().add(new RecordHeader("region", "UAE".getBytes(StandardCharsets.UTF_8)));

        assertThat(filterService.matches(r, List.of(keyRule, headerRule))).isFalse();
    }

    @Test
    void multipleRules_bothMatch_returnsTrue() {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("topic", 0, 0L, "order-99", "payload");
        r.headers().add(new RecordHeader("region", "UAE".getBytes(StandardCharsets.UTF_8)));

        FilterRule keyRule    = new FilterRule(FilterRule.FilterTarget.KEY,    null,     FilterRule.FilterOperator.STARTS_WITH, "order-");
        FilterRule headerRule = new FilterRule(FilterRule.FilterTarget.HEADER, "region", FilterRule.FilterOperator.EQUALS,      "UAE");

        assertThat(filterService.matches(r, List.of(keyRule, headerRule))).isTrue();
    }

    // ---- helpers ----

    private ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>("test-topic", 0, 0L, key, value);
    }

    private ConsumerRecord<String, String> recordWithHeader(String headerKey, String headerValue) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("test-topic", 0, 0L, "some-key", "some-value");
        r.headers().add(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8)));
        return r;
    }
}
