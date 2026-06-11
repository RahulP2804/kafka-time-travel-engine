package com.rahul.kafkatimetravelengine.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * A single filter predicate applied during replay.
 *
 * <p>Supported targets:
 * <ul>
 *   <li>{@code KEY}  — matches against the Kafka message key</li>
 *   <li>{@code HEADER} — matches against a named Kafka header value</li>
 * </ul>
 *
 * <p>Supported operators:
 * <ul>
 *   <li>{@code EQUALS}      — exact string match</li>
 *   <li>{@code CONTAINS}    — substring match</li>
 *   <li>{@code STARTS_WITH} — prefix match</li>
 *   <li>{@code REGEX}       — full Java regex match</li>
 * </ul>
 */
public record FilterRule(

        @NotNull(message = "filterTarget must not be null")
        FilterTarget target,

        /**
         * For HEADER target: the header key to inspect.
         * For KEY target: ignored.
         */
        String headerKey,

        @NotNull(message = "operator must not be null")
        FilterOperator operator,

        @NotBlank(message = "value must not be blank")
        String value
) {

    public enum FilterTarget {
        KEY, HEADER
    }

    public enum FilterOperator {
        EQUALS, CONTAINS, STARTS_WITH, REGEX
    }
}
