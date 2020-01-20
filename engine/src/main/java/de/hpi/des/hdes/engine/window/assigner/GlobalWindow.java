package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.GlobalTimeWindow;
import de.hpi.des.hdes.engine.window.Window;

import java.util.List;

public class GlobalWindow implements WindowAssigner<GlobalTimeWindow> {

  private final GlobalTimeWindow timeWindow = new GlobalTimeWindow();

  @Override
  public List<GlobalTimeWindow> assignWindows(final long timestamp) {
    return List.of(this.timeWindow);
  }

  public static GlobalWindow create() {
    return new GlobalWindow();
  }
}
