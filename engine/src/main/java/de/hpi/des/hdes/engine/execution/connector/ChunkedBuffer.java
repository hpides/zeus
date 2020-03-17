package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ChunkedBuffer<IN> implements Buffer<IN> {


  protected int chunkSize = ExecutionConfig.getConfig().getChunkSize();
  protected final long flushIntervall = ExecutionConfig.getConfig().getFlushIntervallNS(); // in ns
  protected final LinkedBlockingQueue<ArrayDeque<IN>> queue;
  protected ArrayDeque<IN> inChunk;
  protected ArrayDeque<IN> outChunk;
  protected long nextFlushTime;


  public ChunkedBuffer() {
    this.inChunk = new ArrayDeque<>(this.chunkSize);
    this.outChunk = new ArrayDeque<>(this.chunkSize);
    log.info("Chunck Size {}", this.chunkSize);
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public IN poll() {
    if (!this.outChunk.isEmpty()) {
      return this.outChunk.poll();
    } else {
      try {
        final var newChunk = this.queue
            .poll(ExecutionConfig.getConfig().getFlushIntervallMS() / 3 + 1,
                TimeUnit.MILLISECONDS); // We wait for a limited time to assure that operations
        // downstream can continue
        if (newChunk != null) {
          this.outChunk = newChunk;
          return this.outChunk.poll();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  @Override
  public void add(final IN val) {
    this.inChunk.add(val);
    if (this.inChunk.size() >= this.chunkSize) {
      this.flush();
    } else {
      this.flushIfTimeout();
    }
  }

  @Override
  public void flush() {
    if (this.inChunk.isEmpty()) {
      return;
    }
    final var success = this.queue.offer(this.inChunk);
    if (success) {
      this.inChunk = new ArrayDeque<>(this.chunkSize);
    }
  }

  @Override
  public void flushIfTimeout() {
    final var currentTime = System.nanoTime();
    if (this.nextFlushTime <= currentTime && !this.inChunk.isEmpty()) {
      this.flush();
      this.nextFlushTime = currentTime + this.flushIntervall;
    }
  }


  /**
   * Exists just for test purposes. Contains race conditions
   *
   * @return List of all elements in the Buffer, including those in the internal buffers and queue.
   */
  @Override
  public List<IN> unsafePollAll() {
    this.flush();
    final var flattenedQueue = new ArrayList<>(this.outChunk);
    this.queue.forEach(flattenedQueue::addAll);
    return flattenedQueue;
  }

}
