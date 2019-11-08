package de.hpi.des.mpws2019.engine.source;

import java.util.Queue;

public class QueueSource<V> implements Source<V> {
    private Queue<V> queue;

    @Override
    public V poll() {
        return this.queue.poll();
    }
}
