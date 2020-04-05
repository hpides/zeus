package de.hpi.des.hdes.engine.shared.aggregation;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.udf.SharedAggregator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class SharedAggregation<IN, STATE, OUT> extends AbstractTopologyElement<SharedValue<OUT>>
    implements OneInputOperator<SharedValue<IN>, SharedValue<OUT>> {

  private final SharedAggregator<IN, STATE, OUT> aggregator;
  private final WindowAssigner<? extends Window> windowAssigner;
  private final Table<Window, QuerySet, STATE> stateTable;
  private final Map<Window, QuerySet> setMap;

  public SharedAggregation(final SharedAggregator<IN, STATE, OUT> aggregator,
      final WindowAssigner<? extends Window> windowAssigner) {
    this.aggregator = aggregator;
    this.windowAssigner = windowAssigner;
    this.stateTable = HashBasedTable.create();
    this.setMap = new HashMap<>();
  }

  @Override
  public void process(final AData<SharedValue<IN>> in) {
    this.updateState(in);
    if (in.isWatermark()) {
      this.materialize((ADataWatermark<?>) in);
    }
  }

  private void materialize(final ADataWatermark<?> watermark) {
    // get closed windows
    final Set<Window> closedWindows = this.stateTable.rowKeySet().stream()
        .filter(window -> window.getMaxTimestamp() < watermark.getWatermarkTimestamp())
        .collect(Collectors.toSet());

    for (final Window closedWindow : closedWindows) {
      final QuerySet querySet = this.setMap.get(closedWindow);
      final Map<QuerySet, STATE> windowState = this.stateTable.row(closedWindow);
      final List<STATE> aggregates = new ArrayList<>(querySet.length());

      // fill with placeholder
      for (int i = 0; i < querySet.length(); i++) {
        aggregates.add(null);
      }

      // create aggregate for each query
      for (final Entry<QuerySet, STATE> stateEntry : windowState.entrySet()) {
        for (int i = querySet.nextSetBit(0); i != -1; i = querySet.nextSetBit(i + 1)) {
          final STATE state = aggregates.get(i);
          if (state == null) {
            aggregates.set(i, stateEntry.getValue());
          } else {
            final STATE updatedState = this.aggregator.combine(state, stateEntry.getValue());
            aggregates.set(i, updatedState);
          }
        }
      }

      for (int i = 0; i < aggregates.size(); i++) {
        final STATE state = aggregates.get(i);
        final OUT out = this.aggregator.getResult(state);
        final QuerySet set = new QuerySet(querySet.length());
        set.set(i);
        this.collector.collect(AData.of(new SharedValue<>(out, set)));
      }
    }
  }

  private void updateState(final AData<SharedValue<IN>> in) {
    final QuerySet querySet = in.getValue().getQuerySet();
    final List<? extends Window> windows = this.windowAssigner.assignWindows(in.getEventTime());
    for (final Window window : windows) {
      STATE state = this.stateTable.get(window, querySet);
      if (state == null) {
        state = this.aggregator.initialize();
      }
      state = this.aggregator.add(state, in.getValue().getValue());
      this.stateTable.put(window, querySet, state);
    }
  }

}
