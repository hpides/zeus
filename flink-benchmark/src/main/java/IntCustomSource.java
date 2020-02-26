import java.util.Random;
import java.util.stream.IntStream;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class IntCustomSource implements SourceFunction<Tuple1<Integer>> {

  private final int[] buffer;
  private final int eventsPerSecond;
  private final int time;
  private volatile boolean isRunning = true;

  public IntCustomSource(int eps, int time, Random random) {
    this.eventsPerSecond = eps;
    this.time = time;
    this.buffer = IntStream.generate(() -> random.nextInt(10_000)).limit(1_000).toArray();
  }

  @Override
  public void run(SourceContext<Tuple1<Integer>> sourceContext) throws Exception {
    int i = 0;
    int sentEvents = 0;
    long totalEvents = this.eventsPerSecond * this.time;

    while (isRunning && sentEvents < totalEvents) {
      long startTime = System.nanoTime();
      while (sentEvents < totalEvents) {
        final long timeNow = System.nanoTime();
        final long nanoDifference = timeNow - startTime;
        final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
        final long missingEvents = currentEventTarget - sentEvents;

        // Ensures that we don't sent too many events
        final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);
        for (int j = 0; j < eventsToBeSent; j++) {
          int val = this.buffer[i++ % buffer.length];
          sourceContext.collect(new Tuple1<>(val));
        }
        sentEvents += eventsToBeSent;

      }
    }
    System.out.println(sentEvents);
  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
