package de.hpi.des.hdes.engine.execution.slot;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple3;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Event {
    private Tuple data;
    private boolean isWatermark;
    private long timestamp;
}
