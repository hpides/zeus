import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomSink<In> extends RichSinkFunction<In> {

  private long processingTimeDeltaSum = 0L;
  private long benchmarkCount = 0;

  @Getter
  private long totalCount = 0;
  private long minLatency = 0;
  private long maxLatency = 0;

  public double getMinLatency() {
    return minLatency * 1e-6;
  }

  public double getMaxLatency() {
    return maxLatency * 1e-6;
  }

  public void process(final In in) {
    totalCount++;
    long checkEvery = 1;
    if (totalCount % checkEvery == 0) {
      //System.out.println(totalCount);

      long processingTimeDelta = getProcessingTimeDelta(in);
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

  private long getProcessingTimeDelta(In t) {
    if (t instanceof Tuple2) {
      return ((Tuple2<?, Long>) t).f1;
    } else if (t instanceof Tuple3) {
      return ((Tuple3<?, ?, Long>) t).f2;
    } else {
      return 0;
    }
  }


  public double getProcessingLatency() {
    if (this.benchmarkCount > 0) {
      return (processingTimeDeltaSum / (double) this.benchmarkCount) / 1e+6;
    }
    return -1;
  }

  @Override
  public void invoke(In value, Context context) throws Exception {
    process(value);
  }

  @Override
  public void close() throws Exception {
    //sout because logger is problematic with serialization
    System.out.println(this);
    System.out.printf("Total Tuples %,d\n", this.getTotalCount());
    System.out.printf("Average Latency %,.2f Milliseconds\n", this.getProcessingLatency());
    System.out.printf("Min Latency Tuples %,.2f Milliseconds\n", this.getMinLatency());
    System.out.printf("Max Latency Tuples %,.2f Milliseconds\n", this.getMaxLatency());
    super.close();
  }
}
