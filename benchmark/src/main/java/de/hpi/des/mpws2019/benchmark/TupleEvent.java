package de.hpi.des.mpws2019.benchmark;

import lombok.Value;

@Value
public class TupleEvent implements Event {

    private final long key;
    private final int value;

    @Override
    public long getKey() {
        return key;
    }
}
