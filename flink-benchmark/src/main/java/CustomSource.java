import java.util.ArrayList;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomSource<T> implements SourceFunction<T> {

  private static final long serialVersionUID = -9049744310869990871L;
  private final ArrayList<T> buffer;
  private final int eventsPerSecond;
  private final int time;
  private volatile boolean isRunning = true;

  public CustomSource(ArrayList<T> buffer, int eps, int time) {
    this.eventsPerSecond = eps;
    this.time = time;
    this.buffer = buffer;
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    int i = 0;
    int sentEvents = 0;
    long totalEvents = this.eventsPerSecond * this.time;

    while (isRunning && i < buffer.size()) {
      long startTime = System.nanoTime();
      while (sentEvents < totalEvents) {
        final long timeNow = System.nanoTime();
        final long nanoDifference = timeNow - startTime;
        final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
        int j = 0;
        for (; j < currentEventTarget && i < buffer.size(); j++) {
          T val = buffer.get(i++);
          if (val != null) {
            sourceContext.collect(val);
          }
        }
        sentEvents += j;

      }
    }
    System.out.println(sentEvents);
  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
