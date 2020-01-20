package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.Window;
import java.util.List;

public interface WindowAssigner<Window> {
  List<Window> assignWindows(long timestamp);
}
