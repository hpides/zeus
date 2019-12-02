package de.hpi.des.mpws2019.engine.window.assigner;

import de.hpi.des.mpws2019.engine.window.Window;
import java.util.List;

public interface WindowAssigner<W extends Window> {
  List<W> assignWindows(long timestamp);
}
