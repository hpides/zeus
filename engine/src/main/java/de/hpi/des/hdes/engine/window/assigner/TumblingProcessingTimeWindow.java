package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

public class TumblingProcessingTimeWindow implements WindowAssigner<TimeWindow> {

  private final long size;

  protected TumblingProcessingTimeWindow(final long size) {
    this.size = size;
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    final long current = System.currentTimeMillis();
    final long windowStart = current - (current + this.size) % this.size;
    return List.of(new TimeWindow(windowStart, windowStart + this.size));
  }

}
