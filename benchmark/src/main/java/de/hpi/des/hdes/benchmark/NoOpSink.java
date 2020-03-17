package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Sink;

public class NoOpSink<E> implements Sink<E> {
    @Override
    public void process(AData<E> in) {
    }
}
