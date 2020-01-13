package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.HashMap;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
public class TimedBlockingSource<E extends Event> extends AbstractTopologyElement<E> implements
    Source<E> {
    private final HashMap<Long, Long> benchmarkCheckpointToRemoveTime;
    private final HashMap<Long, Long> benchmarkCheckpointToAddTime;
    private final SizedChunkedBuffer<E> queue;

    public TimedBlockingSource(int capacity) {
        this.queue = new SizedChunkedBuffer<>(capacity);
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
        event = queue.poll();
        if (event != null && event.isBenchmarkCheckpoint()) {
            long timestamp = System.nanoTime();
            benchmarkCheckpointToRemoveTime.put(event.getKey(), timestamp);
        }
        if(event != null){
            collector.collect(event);
        }
    }
}