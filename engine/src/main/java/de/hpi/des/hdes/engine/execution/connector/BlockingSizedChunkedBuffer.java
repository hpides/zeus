package de.hpi.des.hdes.engine.execution.connector;

public class BlockingSizedChunkedBuffer<IN> extends SizedChunkedBuffer<IN> {

  public BlockingSizedChunkedBuffer(int maxSize) {
    super(maxSize);
  }

  @Override
  public void add(IN val) {
    while (this.size() >= this.maxSize) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    super.add(val);

  }
}
