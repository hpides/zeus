package de.hpi.des.hdes.engine.window;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import lombok.Getter;

@Getter
public class WatermarkGenerator<T> {

  private long lateness;
  private long interval;
  private long eventCount = 0;

  public WatermarkGenerator(long lateness, long interval) {
    this.lateness = lateness;
    this.interval = interval;
  }

  public static <T> WatermarkGenerator<T> seconds(long secondsLateness, long interval) {
    return new WatermarkGenerator<>(Time.seconds(secondsLateness).getNanos(), interval);
  }

  public AData<T> apply(AData<T> event) {
    eventCount++;
    if (eventCount % interval == 0) {
      return ADataWatermark.from(event, event.getEventTime() - lateness);
    }
    return event;
  }

}
