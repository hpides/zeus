package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class StreamKeyedAggregation<IN, KEY, STATE, OUT>
    extends AbstractTopologyElement<OUT>
    implements OneInputOperator<IN, OUT> {

  private final Aggregator<IN, STATE, OUT> aggregator;
  private final WindowAssigner<Window> windowAssigner;
  private final HashMap<Window, HashMap<KEY, STATE>> windowToState;
  private final KeySelector<IN, KEY> keyselector;

  public StreamKeyedAggregation(final KeySelector<IN, KEY> keyselector,
      final Aggregator<IN, STATE, OUT> aggregator,
      final WindowAssigner<Window> windowAssigner) {
    this.keyselector = keyselector;
    this.aggregator = aggregator;
    this.windowAssigner = windowAssigner;
    this.windowToState = new HashMap<>();
    aggregator.initialize();
  }

  public void closeOutdatedWindows(final List<Window> activeWindows) {
    for (final Window window : this.windowToState.keySet()) {
      if (!activeWindows.contains(window)) {
        final Map<KEY, STATE> keyToState = this.windowToState.get(window);
        for (final STATE state : keyToState.values()) {
          this.collector.collect(AData.of(this.aggregator.getResult(state)));
        }
        this.windowToState.remove(window);
      }
    }
  }

  @Override
  public void process(@NotNull final AData<IN> input) {
    final List<Window> activeWindows = this.windowAssigner.assignWindows(input.getEventTime());
    this.closeOutdatedWindows(activeWindows);
    final IN value = input.getValue();
    final KEY key = this.keyselector.selectKey(value);

    // Add the input to the windows it belongs to
    for (final Window window : activeWindows) {
      final Map<KEY, STATE> inputKeyToState = this.windowToState
          .computeIfAbsent(window, w -> new HashMap<>());
      final STATE state = inputKeyToState.computeIfAbsent(key, k -> this.aggregator.initialize());
      inputKeyToState.put(key, this.aggregator.add(state, value));
    }
  }
}
