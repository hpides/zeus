package de.hpi.des.mpws2019.benchmark;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;

public class TimedConcurrentLinkedQueue extends ConcurrentLinkedQueue<Event> {

    @Getter
    private final HashMap<Long, Long> keyToEventTime;

    public TimedConcurrentLinkedQueue() {
        this.keyToEventTime = new HashMap<Long, Long>();
    }

    public void addTimed(final Event event) {
        final long timestamp = System.nanoTime();
        keyToEventTime.put(event.getKey(), timestamp);
        super.add(event);
    }
}

