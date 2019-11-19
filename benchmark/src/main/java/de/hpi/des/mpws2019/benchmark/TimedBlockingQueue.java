package de.hpi.des.mpws2019.benchmark;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

@Getter
public class TimedBlockingQueue<E extends Event> extends LinkedBlockingQueue<E> {
    private final HashMap<Long, Long> keyToAddTime;
    private final HashMap<Long, Long> keyToRemoveTime;

    public TimedBlockingQueue(int expectedEvents) {
        this(Integer.MAX_VALUE, expectedEvents);
    }

    public TimedBlockingQueue(int capacity, int expectedEvents) {
        super(capacity);
        this.keyToAddTime = new HashMap<>(expectedEvents);
        this.keyToRemoveTime = new HashMap<>(expectedEvents);
    }

    public boolean add(final E event) {
        final long timestamp = System.nanoTime();
        keyToAddTime.put(event.getKey(), timestamp);
        super.add(event);

        return true;
    }

    public E poll() {
        final long timestamp = System.nanoTime();
        E event = null;
        try {
            event = super.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (event != null) {
            keyToRemoveTime.put(event.getKey(), timestamp);
        }
        return event;
    }
}

