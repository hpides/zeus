package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.window.Window;
import java.util.List;

public interface WindowAssigner<W extends Window> {
  List<W> assignWindows(long timestamp);
}
