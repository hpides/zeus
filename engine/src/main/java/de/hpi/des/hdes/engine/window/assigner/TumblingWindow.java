package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

public abstract class TumblingWindow implements WindowAssigner<TimeWindow> {

  private final long size;

  protected TumblingWindow(final long size) {
    this.size = size;
  }

  protected List<TimeWindow> calculateWindow(final long current) {
    final long windowStart = current - (current + this.size) % this.size;
    return List.of(new TimeWindow(windowStart, windowStart + this.size));
  }

  public static TumblingProcessingTimeWindow ofProcessingTime(final Time time) {
    return new TumblingProcessingTimeWindow(time.getNanos());
  }

  public static TumblingEventTimeWindow ofEventTime(final Time time) {
    return new TumblingEventTimeWindow(time.getNanos());
  }

}
