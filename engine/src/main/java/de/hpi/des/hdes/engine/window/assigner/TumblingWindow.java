package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.TimeWindow;

public abstract class TumblingWindow implements WindowAssigner<TimeWindow> {
  private final long size;

  protected TumblingWindow(final long size) {
    this.size = size;
  }

  public static TumblingProcessingTimeWindow ofProcessingTime(final Time time) {
    return new TumblingProcessingTimeWindow(time.getMillis());
  }

  public static TumblingEventTimeWindow ofEventTime(final Time time) {
    return new TumblingEventTimeWindow(time.getMillis());
  }

}
