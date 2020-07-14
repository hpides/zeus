package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.nio.ByteBuffer;
import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.Stoppable;

public class ByteBufferIntSource implements Runnable, Stoppable {

    private final List<Tuple2<Integer, Boolean>> source;
    private final Dispatcher dispatcher;
    private final String pipelineID;
    private long eventCount = 0L;
    private boolean shutdownFlag = false;

    public ByteBufferIntSource(List<Tuple2<Integer, Boolean>> source, Dispatcher dispatcher, String pipelineID) {
        this.source = source;
        this.dispatcher = dispatcher;
        this.pipelineID = pipelineID;
    }

    @Override
    public void shutdown() {
        shutdownFlag = true;
    }

    @Override
    public void run() {
        ByteBuffer eventBuffer = ByteBuffer.allocate(13);
        for (Tuple2<Integer, Boolean> event : source) {
            if (shutdownFlag)
                break;
            eventBuffer.putLong(0, eventCount++).putInt(8, event.v1).put(12, event.v2 ? (byte) 1 : (byte) 0);
            while (!dispatcher.write(pipelineID, eventBuffer.array()))
                ;
        }
    }

}