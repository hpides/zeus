package de.hpi.des.mpws2019.benchmark;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

public class TimedConcurrentBlockingQueue<E extends Event> extends LinkedBlockingQueue<E> {

    @Getter
    private final HashMap<Long, Long> keyToAddTime;
    private final HashMap<Event, Long> eventToRemoveTime;

    public TimedConcurrentBlockingQueue(int expectedEvents) {
        this(Integer.MAX_VALUE, expectedEvents);
    }

    public TimedConcurrentBlockingQueue(int capacity, int expectedEvents) {
        super(capacity);
        this.keyToAddTime = new HashMap<>(expectedEvents);
        this.eventToRemoveTime = new HashMap<>(expectedEvents);
    }

    public boolean add(final E event) {
        final long timestamp = System.nanoTime();
        keyToAddTime.put(event.getKey(), timestamp);
        super.add(event);

        return true;
    }

    public E poll() {
        final long timestamp = System.nanoTime();
        final E event = super.poll();
        if (event != null) {
            eventToRemoveTime.put(event, timestamp);
        }
        return event;
    }

    public HashMap<Long, Long> getKeyToRemoveTimeHashMap() {
        final HashMap<Long, Long> keyToRemoveTime = new HashMap<>();
        for (Event event : eventToRemoveTime.keySet()) {
            keyToRemoveTime.put(event.getKey(), eventToRemoveTime.get(event));
        }
        return keyToRemoveTime;
    }
}

