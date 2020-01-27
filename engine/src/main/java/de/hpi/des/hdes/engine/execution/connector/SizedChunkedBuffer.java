package de.hpi.des.hdes.engine.execution.connector;

import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

public class SizedChunkedBuffer<IN> extends ChunkedBuffer<IN> {

  private final AtomicInteger currentSize = new AtomicInteger(0);
  private final int maxSize;

  public SizedChunkedBuffer(final int maxSize) {
    this.maxSize = maxSize;
  }

  public SizedChunkedBuffer() {
    this(Integer.MAX_VALUE);
  }

  public int size() {
    return this.currentSize.get();
  }

  @Override
  public void add(final IN val) {
    if (this.currentSize.get() > this.maxSize) {
      throw new IllegalStateException(
          String.format("Queue is full. Size is %d", this.currentSize.get()));
    }
    this.currentSize.incrementAndGet();
    super.add(val);
    // not quite thread safe as the thread could be suspended in between. However, add should always
    // succeed and this resolution should be fine for our purpose
  }

  @Nullable
  @Override
  public IN poll() {
    final IN pollResult = super.poll();
    if (pollResult != null) {
      this.currentSize.decrementAndGet();
    }
    return pollResult;
  }
}
