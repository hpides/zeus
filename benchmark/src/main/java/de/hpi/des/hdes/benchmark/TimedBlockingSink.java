package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.HashMap;
import lombok.Getter;

@Getter
public class TimedBlockingSink<E extends Event> implements Sink<E> {
    private final HashMap<Long, Long> benchmarkCheckpointToAddTime;
    private long sinkSize;
    private final Collector<E> collector;


    public TimedBlockingSink() {
        this.benchmarkCheckpointToAddTime = new HashMap<>();
        this.sinkSize = 0;
        this.collector = this::process;
    }

    public long size() {
        return sinkSize;
    }

    @Override
    public void process(AData<E> event) {
        if (event.getValue().isBenchmarkCheckpoint()) {
            final long timestamp = System.nanoTime();
            benchmarkCheckpointToAddTime.put(event.getValue().getKey(), timestamp);
        }
        sinkSize++;
    }
}
