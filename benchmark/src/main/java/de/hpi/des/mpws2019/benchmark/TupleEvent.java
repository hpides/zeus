package de.hpi.des.mpws2019.benchmark;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class TupleEvent implements Event {

    private long key;
    private int value;

    @Override
    public long getKey() {
        return key;
    }

}
