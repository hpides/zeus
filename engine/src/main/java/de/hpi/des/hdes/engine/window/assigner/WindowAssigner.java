package de.hpi.des.hdes.engine.window.assigner;

import java.util.List;

public interface WindowAssigner<Window> {

  List<Window> assignWindows(long timestamp);

  long nextWindowStart(long watermark);

  long maximumSliceSize();
}
