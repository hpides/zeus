package de.hpi.des.hdes.engine.execution.connector;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SizedChunkedBuffer<IN> extends ChunkedBuffer<IN> {

  private final AtomicInteger currentSize = new AtomicInteger(0);
  protected final int maxSize;

  public SizedChunkedBuffer(final int maxSize) {
    this.maxSize = Math.max(maxSize, this.chunkSize);
  }

  public SizedChunkedBuffer() {
    this(Integer.MAX_VALUE);
  }

  public int size() {
    return this.currentSize.get();
  }

  public boolean isFull() {
    return this.size() == maxSize;
  }

  @Override
  public void add(final IN val) {
    if (this.currentSize.get() >= this.maxSize) {
      throw new IllegalStateException(
          String.format("Queue is full. Size is %d", this.currentSize.get()));
    }
    this.currentSize.incrementAndGet();
    super.add(val);
    // not quite thread safe as the thread could be suspended in between. However, add should always
    // succeed and this resolution should be fine for our purpose
  }

  public void drop() {
    this.inChunk.clear();
    this.queue.clear();
    this.outChunk.clear();
    this.currentSize.set(0);
  }

  @Override
  public IN poll() {
    final IN pollResult = super.poll();
    if (pollResult != null) {
      this.currentSize.decrementAndGet();
    }
//    if (this.size() <= this.maxSize - this.chunkSize) {
//      // leave some space so we do not lock constantly
//      synchronized (this) {
//        this.notify();
//      }
//    }
    return pollResult;
  }
}
