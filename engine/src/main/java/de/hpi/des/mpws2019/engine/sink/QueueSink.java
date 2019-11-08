package de.hpi.des.mpws2019.engine.sink;

import java.util.Queue;

public class QueueSink<V> implements Sink<V> {
    private Queue<V> queue;

    @Override
    public void write(final V input) {
        this.queue.add(input);
    }
}
