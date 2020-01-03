package de.hpi.des.hdes.engine.execution.connector;

public class AdaptiveChunkedBuffer<In> extends ChunkedBuffer<In> {

  private boolean timedOut = false;
  private final int MAX_CHUNK_SIZE = 10_000_000; // 10 MB of pointers
  private final int MIN_CHUNK_SIZE = 10;
  private int lowerMargin = MIN_CHUNK_SIZE;
  private int upperMargin = MAX_CHUNK_SIZE;
  private int currentChunkSize = Math.floorDiv(lowerMargin + upperMargin, 2);
  private int lastChunkSize = currentChunkSize;


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
      if (lastChunkSize < currentChunkSize) {
        currentChunkSize = lastChunkSize;
        upperMargin = currentChunkSize - MIN_CHUNK_SIZE; // This will lead a smaller ChunkSize in
        // the next run, as the last one was to high.
      } else {
        lowerMargin = MIN_CHUNK_SIZE;
        upperMargin = currentChunkSize - 1;
        if (upperMargin < MIN_CHUNK_SIZE) {
          upperMargin = MIN_CHUNK_SIZE;
        }
        currentChunkSize = Math.floorDiv(lowerMargin + upperMargin, 2);
      }
    } else {
      lastChunkSize = currentChunkSize;
      lowerMargin = currentChunkSize + 1;
      if (lowerMargin >= upperMargin) {
        upperMargin = upperMargin + MIN_CHUNK_SIZE;
        if (upperMargin > MAX_CHUNK_SIZE) {
          upperMargin = MAX_CHUNK_SIZE;
        }
      }
      currentChunkSize = Math.floorDiv(lowerMargin + upperMargin, 2);
    }
    this.chunkSize = currentChunkSize;
  }

  @Override
  public void flushIfTimeout() {
    var currentTime = System.nanoTime();
    if (this.nextFlushTime <= currentTime && !inChunk.isEmpty()) {
      this.flush();
      this.nextFlushTime = currentTime + this.flushIntervall;
      this.timedOut = true;
    }
  }
}
