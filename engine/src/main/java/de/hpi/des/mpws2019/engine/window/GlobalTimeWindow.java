package de.hpi.des.mpws2019.engine.window;

public class GlobalTimeWindow implements Window {

  @Override
  public long getMaxTimestamp() {
    return Long.MAX_VALUE;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof GlobalTimeWindow;
  }
}
