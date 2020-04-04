package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

/**
 * A tumbling window which uses the event time and assigns it to windows.
 */
public class TumblingEventTimeWindow extends TumblingWindow {

  public TumblingEventTimeWindow(final long size) {
    super(size);
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    return this.calculateWindow(timestamp);
  }
}
