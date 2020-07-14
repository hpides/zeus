package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.nio.ByteBuffer;
import java.util.List;

import org.jooq.lambda.tuple.Tuple4;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.Stoppable;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;

public class ByteBufferIntListSink implements Runnable, Stoppable {
    private final List<Tuple4<Long, Integer, Integer, Boolean>> resultList;
    private boolean shutdownFlag = false;
    private final Dispatcher dispatcher;
    private final ReadBuffer input;

    public ByteBufferIntListSink(Dispatcher dispatcher, ReadBuffer input,
            List<Tuple4<Long, Integer, Integer, Boolean>> resultList) {
        this.dispatcher = dispatcher;
        this.input = input;
        this.resultList = resultList;
    }

    @Override
    public void shutdown() {
        shutdownFlag = true;
    }

    @Override
    public void run() {
        int initialPosition;
        while (!shutdownFlag) {
            if (!input.hasRemaining()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
                continue;
            }
            initialPosition = input.getBuffer().position();
            resultList.add(
                    new Tuple4<Long, Integer, Integer, Boolean>(input.getBuffer().getLong(), input.getBuffer().getInt(),
                            input.getBuffer().getInt(), input.getBuffer().get() == (byte) 1 ? true : false));
            dispatcher.free(input, initialPosition);
        }
    }

}