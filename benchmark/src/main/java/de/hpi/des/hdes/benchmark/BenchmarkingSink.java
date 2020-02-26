package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Sink;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;

@Log4j2
@NoArgsConstructor
public class BenchmarkingSink<E> implements Sink<E> {

  private String name = "";
  private long processingTimeDeltaSum = 0L;
  private long benchmarkCount = 0;

  @Getter
  private long totalCount = 0;

  public double getMinLatency() {
    return minLatency * 1e-6;
  }

  public double getMaxLatency() {
    return maxLatency * 1e-6;
  }

  private long minLatency = 0;
  private long maxLatency = 0;

  public BenchmarkingSink(String name) {
    this.name = name;
  }

  @Override
  public void process(final AData<E> in) {
    totalCount++;
    long checkEvery = 1;
    if (totalCount % checkEvery == 0) {

      long processingTimeDelta = getProcessingTimeDelta(in.getValue());
      if (processingTimeDelta == 0) {
        return;
      }
      processingTimeDeltaSum += processingTimeDelta;
      if (processingTimeDelta < minLatency) {
        minLatency = processingTimeDelta;
      }
      if (processingTimeDelta > maxLatency) {
        maxLatency = processingTimeDelta;
      }
      benchmarkCount++;
    }
  }

  private long getProcessingTimeDelta(E t) {
    if (t instanceof Tuple2) {
      return ((Tuple2<?, Long>) t).v2;
    } else if (t instanceof Tuple3) {
      return ((Tuple3<?, ?, Long>) t).v3;
    } else {
      log.warn("Not able to cast event");
      return 0;
    }
  }


  public double getProcessingLatency() {
    if (this.benchmarkCount > 0) {
      return (processingTimeDeltaSum / (double) this.benchmarkCount) / 1e+6;
    }
    return -1;
  }

  public void log() {
    log.info("{}\n", this.name);
    log.printf(Level.INFO, "Total Tuples %,d", this.getTotalCount());
    log.printf(Level.INFO, "Average Latency %,.2f Milliseconds", this.getProcessingLatency());
    log.printf(Level.INFO, "Min Latency Tuples %,.2f Milliseconds", this.getMinLatency());
    log.printf(Level.INFO, "Max Latency Tuples %,.2f Milliseconds", this.getMaxLatency());
  }
}
