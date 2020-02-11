package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;

public class TumblingProcessingTimeWindow extends TumblingWindow {

  public TumblingProcessingTimeWindow(final long size) {
    super(size);
  }

  @Override
  public List<TimeWindow> assignWindows(final long timestamp) {
    return this.calculateWindow(System.nanoTime());
  }

}