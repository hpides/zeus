package de.hpi.des.hdes.benchmark;

import java.util.LinkedList;
import java.util.List;

import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.execution.slot.Event;
import de.hpi.des.hdes.engine.io.Buffer;

public class NetworkBuffer implements Buffer {

    private final SizedChunkedBuffer<Object> queue;

    public NetworkBuffer(SizedChunkedBuffer<Object> queue) {
        this.queue = queue;
    }

    @Override
    public List<Event> poll() {
        List<Event> result = new LinkedList<Event>();
        Object nextObject = queue.poll();
        while (nextObject != null) {
            // TODO
            nextObject = queue.poll();
        }
        return result;
    }

}