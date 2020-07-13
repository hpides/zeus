package de.hpi.des.hdes.engine.operation;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AggregateFunction {
    AVERAGE("(state * count + current) / (count + 1)", true), COUNT("state + 1", false),
    MINIMUM("Math.min(current, state)", false), MAXIMUM("Math.max(current, state)", false),
    SUM("current + state", false);

    private final String aggregateImplementation;
    private final boolean shouldCountPerWindow;
}