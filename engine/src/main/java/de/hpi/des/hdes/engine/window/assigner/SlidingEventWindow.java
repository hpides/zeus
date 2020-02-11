package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

public class SlidingEventWindow extends SlidingWindow {

  protected SlidingEventWindow(final long slide, final long length) {
    super(slide, length);
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    return this.calculateWindows(timestamp);
  }
}
