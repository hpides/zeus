package de.hpi.des.hdes.engine.shared.aggregation;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class SharedSession {

  private final TumblingWindow sliceAssigner;

  private final Table<Window, SharedQuery, Integer> indexTable;
  private final Map<Window, BitSet> changes;
  private final Table<Window, Window, BitSet> queryTable;


  public SharedSession(final TumblingWindow sliceAssigner) {
    this.sliceAssigner = sliceAssigner;
    this.indexTable = HashBasedTable.create();
    this.changes = new HashMap<>();
    this.queryTable = HashBasedTable.create();
  }

  public void addQuery(final SharedQuery query) {
    final Window window = this.sliceAssigner.assignWindows(query.getCreationTimestamp()).get(0);

    final BitSet changeCode = this.changes.computeIfAbsent(window, key -> new BitSet());
    final int length = changeCode.length();
    this.indexTable.put(window, query, length);
    changeCode.set(length);
  }

  public void removeQuery(final SharedQuery query) {
    final Window window = this.sliceAssigner.assignWindows(query.getCreationTimestamp()).get(0);
    final long diff = query.getCreationTimestamp() - this.sliceAssigner.maximumSliceSize();
    final Window previousWindow = this.sliceAssigner.assignWindows(diff).get(0);

    final BitSet changeCode = this.changes.get(window);
    final int index = this.indexTable.get(window, query);
    changeCode.flip(index);
  }
}
