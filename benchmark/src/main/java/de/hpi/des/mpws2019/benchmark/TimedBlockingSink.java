package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.engine.operation.AbstractOperation;
import de.hpi.des.mpws2019.engine.operation.Collector;
import de.hpi.des.mpws2019.engine.operation.Sink;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

@Getter
public class TimedBlockingSink<E extends Event> implements Sink<E> {
    private final HashMap<Long, Long> benchmarkCheckpointToAddTime;
    private final LinkedBlockingQueue<E> queue;
    private long sinkSize;
    private final Collector<E> collector;

    @Override
    public void init(Collector<E> collector) {

    }

    public TimedBlockingSink() {
        this.queue = new LinkedBlockingQueue<>();
        this.benchmarkCheckpointToAddTime = new HashMap<>();
        this.sinkSize = 0;

        this.collector = queue::add;
    }

    public long size() {
        return sinkSize;
    }

    @Override
    public void process(E event) {
        if(event.isBenchmarkCheckpoint()) {
            final long timestamp = System.nanoTime();
            benchmarkCheckpointToAddTime.put(event.getKey(), timestamp);
        }
        sinkSize++;
    }
}
