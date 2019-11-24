package de.hpi.des.mpws2019.engine.execution.slot;

import java.util.Queue;

public class QueueInput<In> implements Input<In> {

  private Queue<In> queue;

  public QueueInput(final Queue<In> queue) {
    this.queue = queue;
  }

  @Override
  public In poll() {
    return queue.poll();
  }
}
