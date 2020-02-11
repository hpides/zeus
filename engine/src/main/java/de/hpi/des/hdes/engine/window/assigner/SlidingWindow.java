package de.hpi.des.hdes.engine.window.assigner;

import com.google.common.annotations.VisibleForTesting;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.ArrayList;
import java.util.List;

public abstract class SlidingWindow implements WindowAssigner<TimeWindow> {

  private final long slide;
  private final long size;

  protected SlidingWindow(final long slide, final long size) {
    this.slide = slide;
    this.size = size;
  }

  protected List<TimeWindow> calculateWindows(final long current) {
    final List<TimeWindow> timeWindows = new ArrayList<>((int) (this.size / this.slide));
    final long windowStart = current - (current + this.slide + this.size) % this.size;
    for (long currentStart = windowStart; currentStart > current - this.size;
        currentStart -= this.slide) {
      timeWindows.add(new TimeWindow(currentStart, currentStart + this.size));
    }
    return timeWindows;
  }

  public static SlidingProcessingWindow ofProcessingTime(final Time length, final Time slide) {
    return new SlidingProcessingWindow(slide.getNanos(), slide.getNanos());
  }

  public static SlidingEventWindow ofEventTime(final Time length, final Time slide) {
    return new SlidingEventWindow(slide.getNanos(), length.getNanos());
  }

  @VisibleForTesting
  public static SlidingProcessingWindow ofProcessingTime(final long length, final long slide) {
    return new SlidingProcessingWindow(slide, slide);
  }

  @VisibleForTesting
  public static SlidingEventWindow ofEventTime(final long length, final long slide) {
    return new SlidingEventWindow(slide, length);
  }

}
