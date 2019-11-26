package de.hpi.des.mpws2019.engine.execution.slot;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;

public class QueueBuffer<VAL> implements InputBuffer<VAL> {

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

  public void add(VAL val) {
    this.queue.add(val);
  }
}
