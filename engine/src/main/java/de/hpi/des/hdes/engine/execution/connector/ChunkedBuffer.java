package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.Nullable;

@Log4j2
public class ChunkedBuffer<IN> implements Buffer<IN> {


  protected int chunkSize = ExecutionConfig.getConfig().getChunkSize();
  protected final long flushIntervall = ExecutionConfig.getConfig().getFlushIntervallNS(); // in ns
  protected ArrayDeque<IN> inChunk;
  protected ArrayDeque<IN> outChunk;
  protected final LinkedBlockingQueue<ArrayDeque<IN>> queue;
  protected long nextFlushTime;



  public ChunkedBuffer() {
    this.inChunk = new ArrayDeque<>(chunkSize);
    this.outChunk = new ArrayDeque<>();
    this.queue = new LinkedBlockingQueue<>();
  }

  @Nullable
  @Override
  public IN poll() {
    if(!this.outChunk.isEmpty()){
      return outChunk.poll();
    } else {
      try {
        var newChunk = queue.poll(ExecutionConfig.getConfig().getFlushIntervallMS()/3 + 1,
            TimeUnit.MILLISECONDS); // We wait for a limited time to assure
        if (newChunk != null){
          this.outChunk = newChunk;
          return this.outChunk.poll();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  @Override
  public void add(IN val) {
    this.inChunk.add(val);
    if(inChunk.size() >= chunkSize){
      this.flush();
    } else {
      this.flushIfTimeout();
    }
  }

  @Override
  public void flush(){
    if (this.inChunk.isEmpty()){
      return;
    }
    var success = this.queue.offer(this.inChunk);
    if(success){
      this.inChunk = new ArrayDeque<>(chunkSize);
    }
  }

  @Override
  public void flushIfTimeout() {
    var currentTime = System.nanoTime();
    if (this.nextFlushTime <= currentTime && !inChunk.isEmpty()){
      this.flush();
      this.nextFlushTime = currentTime + this.flushIntervall;
    }
  }


  /**
   * Exists just for test purposes. Contains race conditions
   */
  @Override
  public List<IN> unsafePollAll() {
    this.flush();
    var flattenedQueue = new ArrayList<>(this.outChunk);
    this.queue.forEach(flattenedQueue::addAll);
    return flattenedQueue;
  }

}
