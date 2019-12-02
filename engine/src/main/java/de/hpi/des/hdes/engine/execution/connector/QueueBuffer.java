package de.hpi.des.hdes.engine.execution.connector;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;

public class QueueBuffer<VAL> implements Buffer<VAL> {

  @Getter
  private final Queue<VAL> queue;

  public QueueBuffer() {
    this.queue = new ConcurrentLinkedQueue<>();
  }

  public QueueBuffer(final Queue<VAL> queue) {
    this.queue = queue;
  }

  @Override
  public VAL poll() {
    return this.queue.poll();
  }

  @Override
  public void add(VAL val) {
    this.queue.add(val);
  }
}
