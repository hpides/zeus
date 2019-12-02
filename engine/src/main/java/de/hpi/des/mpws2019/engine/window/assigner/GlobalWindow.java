package de.hpi.des.mpws2019.engine.window.assigner;

import de.hpi.des.mpws2019.engine.window.GlobalTimeWindow;
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
