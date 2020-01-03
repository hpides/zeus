package de.hpi.des.hdes.engine.execution.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.Nullable;

public class QueueBuffer<VAL> implements Buffer<VAL> {

  private final ConcurrentLinkedQueue<VAL> queue;

  public QueueBuffer() {
    this.queue = new ConcurrentLinkedQueue<>();
  }

  public QueueBuffer(final Queue<VAL> queue) {
    this.queue = new ConcurrentLinkedQueue<>(queue);
  }

  @Nullable
  @Override
  public VAL poll() {
    return this.queue.poll();
  }

  @Override
  public List<VAL> unsafePollAll() {
    return new ArrayList<>(this.queue);
  }

  @Override
  public void add(VAL val) {
    this.queue.add(val);
  }
}
