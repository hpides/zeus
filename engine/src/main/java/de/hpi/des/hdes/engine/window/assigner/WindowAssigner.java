package de.hpi.des.hdes.engine.window.assigner;

import java.util.List;

public interface WindowAssigner<Window> {

  /**
   * Assign a timestamp to a window instance.
   *
   * @return list of windows that timestamps are assigned to
   */
  List<Window> assignWindows(long timestamp);

  /**
   * Returns the start time of the next window after the watermark timestamp.
   *
   * @param watermark watermark timestamp
   * @return start time of the next window
   */
  long nextWindowStart(long watermark);

  long maximumSliceSize();
}
