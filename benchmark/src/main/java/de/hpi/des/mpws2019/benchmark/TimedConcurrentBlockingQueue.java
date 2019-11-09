package de.hpi.des.mpws2019.benchmark;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;

public class TimedConcurrentBlockingQueue<E extends Event> extends LinkedBlockingQueue<E> {

  @Getter
  private final HashMap<Long, Long> keyToEventTime;

  public TimedConcurrentBlockingQueue(int expectedEvents) {
    this(Integer.MAX_VALUE, expectedEvents);
  }

  public TimedConcurrentBlockingQueue(int capacity, int expectedEvents) {
    super(capacity);
    this.keyToEventTime = new HashMap<Long, Long>(expectedEvents);
  }

  public boolean add(final E event) {
    final long timestamp = System.nanoTime();
    keyToEventTime.put(event.getKey(), timestamp);
    super.add(event);

    return true;
  }
}

