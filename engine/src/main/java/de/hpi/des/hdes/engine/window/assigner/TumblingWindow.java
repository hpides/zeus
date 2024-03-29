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

  /**
   * Factory method for TumblingProcessingTimeWindow
   *
   * @see TumblingProcessingTimeWindow
   */
  public static TumblingProcessingTimeWindow ofProcessingTime(final Time time) {
    return new TumblingProcessingTimeWindow(time.getNanos());
  }

  /**
   * Factory method for TumblingEventTimeWindow
   *
   * @see TumblingEventTimeWindow
   */
  public static TumblingEventTimeWindow ofEventTime(final long time) {
    return new TumblingEventTimeWindow(time);
  }

  /**
   * Factory method for TumblingEventTimeWindow
   *
   * @see TumblingEventTimeWindow
   */
  public static TumblingEventTimeWindow ofEventTime(final Time time) {
    return new TumblingEventTimeWindow(time.getNanos());
  }

  @Override
  public long nextWindowStart(final long watermark) {
    final long windowStart = watermark - (watermark + this.size) % this.size;
    return windowStart - this.size;
  }

  @Override
  public long maximumSliceSize() {
    return this.size;
  }
}

