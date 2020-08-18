package de.hpi.des.hdes.engine.execution.logdata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class AJoinReadData {
    final private long startTime;
    final private long diffTime;
    final private int eventCount;
    final private String side;
}