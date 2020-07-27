package de.hpi.des.hdes.benchmark.generator;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import de.hpi.des.hdes.benchmark.nexmark.NexmarkLightDataGenerator;

public class NexmarkByteBidGenerator extends UniformGenerator<byte[]> {
  NexmarkLightDataGenerator generator;
  private ByteBuffer buffer = ByteBuffer.allocate(33);
  private long watermarkInterval = 100;
  private long lastWatermark = 0;

  public NexmarkByteBidGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor,
      NexmarkLightDataGenerator generator) {
    super(eventsPerSecond, timeInSeconds, executor);
    this.generator = generator;
  }

  @Override
  protected byte[] generateEvent() {
    var tuple = generator.generateBid();
    long time = System.currentTimeMillis();
    byte watermark = 0;
    if (time - lastWatermark > watermarkInterval) {
      lastWatermark = time;
      watermark = 1;
      time -= 500;
    }
    buffer.clear();
    buffer.putLong(tuple.v5).putLong(tuple.v1).putLong(tuple.v2).putInt(tuple.v3).putInt(tuple.v4).put(watermark);

    return buffer.array();
  }
}