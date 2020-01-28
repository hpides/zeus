package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.AData;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.Nullable;

public class QueueBuffer<VAL> implements Buffer<VAL> {

  private final ConcurrentLinkedQueue<AData<VAL>> queue;

  public QueueBuffer() {
    this.queue = new ConcurrentLinkedQueue<>();
  }

  public QueueBuffer(final Queue<AData<VAL>> queue) {
    this.queue = new ConcurrentLinkedQueue<>(queue);
  }

  @Nullable
  @Override
  public AData<VAL> poll() {
    return this.queue.poll();
  }

  @Override
  public List<AData<VAL>> unsafePollAll() {
    return new ArrayList<>(this.queue);
  }

  @Override
  public void add(final AData<VAL> val) {
    this.queue.add(val);
  }
}
