package de.hpi.des.hdes.engine.execution.slot;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple3;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Event {
    private Object data;
    private boolean isWatermark;
    private long timestamp;
}
