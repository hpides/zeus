package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.Output;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;

public class QueueBuffer<VAL> implements Output<VAL>, Input<VAL> {

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
  public void collect(final VAL val) {
    this.queue.add(val);
  }
}
