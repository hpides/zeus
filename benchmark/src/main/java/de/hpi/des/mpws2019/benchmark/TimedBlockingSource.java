package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.engine.operation.AbstractInitializable;
import de.hpi.des.mpws2019.engine.operation.Source;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

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
            System.out.println(String.format("Polling from TimedBlockingSource containing %s was interrupted", this));
            Thread.currentThread().interrupt();
        }
        if (event != null && event.isBenchmarkCheckpoint()) {
            long timestamp = System.nanoTime();
            benchmarkCheckpointToRemoveTime.put(event.getKey(), timestamp);
        }
        collector.collect(event);
    }
}
