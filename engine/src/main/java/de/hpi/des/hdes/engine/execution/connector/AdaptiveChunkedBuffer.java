package de.hpi.des.hdes.engine.execution.connector;

public class AdaptiveChunkedBuffer<In> extends ChunkedBuffer<In> {

  private boolean timedOut = false;
  private static final int MAX_CHUNK_SIZE = 10_000_000; // 10 MB of pointers
  private static final int MIN_CHUNK_SIZE = 10;
  private int lowerMargin = MIN_CHUNK_SIZE;
  private int upperMargin = MAX_CHUNK_SIZE;
  private int currentChunkSize = Math.floorDiv(this.lowerMargin + this.upperMargin, 2);
  private int lastChunkSize = this.currentChunkSize;


  @Override
  public void flush() {
    super.flush();
    this.determineNewChunkSize();
  }

  /**
   * This method basically performs a binary search to determine a ChunkSize that is doable before
   * timing out. However, the lower and upper margins are sometimes reset as there is no 'right'
   * value to be determined and the 'right' ChunkSize depends on the current load.
   */
  private void determineNewChunkSize() {
    if (this.timedOut) {
      this.timedOut = false;
      if (this.lastChunkSize < this.currentChunkSize) {
        this.currentChunkSize = this.lastChunkSize;
        this.upperMargin =
            this.currentChunkSize - MIN_CHUNK_SIZE; // This will lead a smaller ChunkSize in
        // the next run, as the last one was to high.
      } else {
        this.lowerMargin = MIN_CHUNK_SIZE;
        this.upperMargin = this.currentChunkSize - 1;
        if (this.upperMargin < MIN_CHUNK_SIZE) {
          this.upperMargin = MIN_CHUNK_SIZE;
        }
        this.currentChunkSize = Math.floorDiv(this.lowerMargin + this.upperMargin, 2);
      }
    } else {
      this.lastChunkSize = this.currentChunkSize;
      this.lowerMargin = this.currentChunkSize + 1;
      if (this.lowerMargin >= this.upperMargin) {
        this.upperMargin += MIN_CHUNK_SIZE;
        if (this.upperMargin > MAX_CHUNK_SIZE) {
          this.upperMargin = MAX_CHUNK_SIZE;
        }
      }
      this.currentChunkSize = Math.floorDiv(this.lowerMargin + this.upperMargin, 2);
    }
    this.chunkSize = this.currentChunkSize;
  }

  @Override
  public void flushIfTimeout() {
    final var currentTime = System.nanoTime();
    if (this.nextFlushTime <= currentTime && !this.inChunk.isEmpty()) {
      this.flush();
      this.nextFlushTime = currentTime + this.flushIntervall;
      this.timedOut = true;
    }
  }
}
