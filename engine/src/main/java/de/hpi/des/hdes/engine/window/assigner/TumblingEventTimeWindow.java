package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

public class TumblingEventTimeWindow implements WindowAssigner<TimeWindow> {

  private final long size;

  protected TumblingEventTimeWindow(final long size) {
    this.size = size;
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    final long windowStart = timestamp - (timestamp + this.size) % this.size;
    return List.of(new TimeWindow(windowStart, windowStart + this.size));
  }
}
