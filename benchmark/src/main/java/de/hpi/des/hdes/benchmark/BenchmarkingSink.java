package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BenchmarkingSink<E> implements Sink<E> {

  private long ingestionTimeDeltaSum = 0L;
  private long benchmarkCount = 0;

  @Getter
  private long totalCount = 0;
  @Getter
  private long minLatency = 0;
  @Getter
  private long maxLatency = 0;

  @Override
  public void process(final AData<E> in) {
    totalCount++;
    long checkEvery = 1_000;
    if (totalCount % checkEvery == 0) {
      long currentTime = System.nanoTime();
      var ingestionTime = in.getIngestionTime();
      if (ingestionTime == 0) {
        return;
      }
      var delta = (currentTime - ingestionTime);
      ingestionTimeDeltaSum += delta;
      if (delta < minLatency) {
        minLatency = delta;
      }
      if (delta > maxLatency) {
        maxLatency = delta;
      }
      benchmarkCount++;
      //log.info(in);
    }
  }

  public long getIngestionLatency() {
    if (this.benchmarkCount > 0) {
      return TimeUnit.NANOSECONDS.toMillis(ingestionTimeDeltaSum / this.benchmarkCount);
    }
    return -1;
  }
}
