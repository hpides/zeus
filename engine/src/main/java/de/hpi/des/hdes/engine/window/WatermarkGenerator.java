package de.hpi.des.hdes.engine.window;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 *
 * Stateful generator for watermarks.
 *
 * @param <T> Type of the incoming events.
 */
@Getter
public class WatermarkGenerator<T> {

  private long lateness;
  private long interval;
  private long eventCount = 0;

  /**
   *
   * @param lateness allowed lateness for events
   * @param interval a watermark is emitted after each interval
   */
  public WatermarkGenerator(long lateness, long interval) {
    this.lateness = lateness;
    this.interval = interval;
  }

  /**
   * Factory method.
   *
   * @see #WatermarkGenerator(long, long)
   */
  public static <T> WatermarkGenerator<T> seconds(long secondsLateness, long interval) {
    return new WatermarkGenerator<>(Time.seconds(secondsLateness).getNanos(), interval);
  }

  /**
   * Factory method.
   *
   * @see #WatermarkGenerator(long, long)
   */
  public static <T> WatermarkGenerator<T> milliseconds(long lateness, long interval) {
    return new WatermarkGenerator<>(TimeUnit.MILLISECONDS.toNanos(lateness), interval);
  }

  /**
   * Transforms elements to watermarks after each interval o.w. it just passes through the event.
   *
   * @param event element to pass through or mark as watermark
   * @return transformed or passed through event
   */
  public AData<T> apply(AData<T> event) {
    eventCount++;
    if (eventCount % interval == 0) {
      return ADataWatermark.from(event, event.getEventTime() - lateness);
    }
    return event;
  }

}
