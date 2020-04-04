package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

/**
 * Sliding window which uses current time to assign an event to a window.
 */
public class SlidingProcessingWindow extends SlidingWindow {

  protected SlidingProcessingWindow(final long slide, final long length) {
    super(slide, length);
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    return this.calculateWindows(System.nanoTime());
  }
}
