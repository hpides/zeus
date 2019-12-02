package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.operation.AbstractInitializable;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
public class TimedBlockingSource<E extends Event> extends AbstractInitializable<E> implements
    Source<E> {
    private final HashMap<Long, Long> benchmarkCheckpointToRemoveTime;
    private final HashMap<Long, Long> benchmarkCheckpointToAddTime;
    private final LinkedBlockingQueue<E> queue;

    public TimedBlockingSource(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.benchmarkCheckpointToRemoveTime = new HashMap<>();
        this.benchmarkCheckpointToAddTime = new HashMap<>();
    }

    public TimedBlockingSource() {
        this(Integer.MAX_VALUE);
    }

    public void offer(E event) {
        if(event.isBenchmarkCheckpoint()) {
            final long timestamp = System.nanoTime();
            benchmarkCheckpointToAddTime.put(event.getKey(), timestamp);
        }
        queue.add(event);
    }

    @Override
    public void read() {
        E event = null;
        try {
            event = queue.take();
        } catch (InterruptedException e) {
            log.info("Polling from TimedBlockingSource containing {} was interrupted", this);
            Thread.currentThread().interrupt();
        }
        if (event != null && event.isBenchmarkCheckpoint()) {
            long timestamp = System.nanoTime();
            benchmarkCheckpointToRemoveTime.put(event.getKey(), timestamp);
        }
        collector.collect(event);
    }
}