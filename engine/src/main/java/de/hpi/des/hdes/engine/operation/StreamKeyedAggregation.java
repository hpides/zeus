package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/**
 * Operator that aggregates the stream grouped by key
 *
 * @param <IN>    the type of the input elements
 * @param <KEY>   the type of the key
 * @param <STATE> the type of the state
 * @param <OUT>   the type of output elements
 */
@Slf4j
public class StreamKeyedAggregation<IN, KEY, STATE, OUT>
    extends AbstractTopologyElement<OUT>
    implements OneInputOperator<IN, OUT> {

  private final Aggregator<IN, STATE, OUT> aggregator;
  private final WindowAssigner<Window> windowAssigner;
  private final WatermarkGenerator<OUT> watermarkGenerator;
  private final TimestampExtractor<OUT> timestampExtractor;
  private final HashMap<Window, HashMap<KEY, STATE>> windowToState;
  private final KeySelector<IN, KEY> keyselector;
  private long latestTimestamp = 0;

  public StreamKeyedAggregation(final KeySelector<IN, KEY> keyselector,
      final Aggregator<IN, STATE, OUT> aggregator,
      final WindowAssigner<Window> windowAssigner,
      final WatermarkGenerator<OUT> watermarkGenerator,
      final TimestampExtractor<OUT> timestampExtractor) {
    this.keyselector = keyselector;
    this.aggregator = aggregator;
    this.windowAssigner = windowAssigner;
    this.watermarkGenerator = watermarkGenerator;
    this.timestampExtractor = timestampExtractor;

    this.windowToState = new HashMap<>();
    aggregator.initialize();
  }

  public void processWatermark(final AData<IN> in) {
    if (in.isWatermark()) {
      final long newTimestamp = ((ADataWatermark<?>) (in)).getWatermarkTimestamp();
      if (newTimestamp <= this.latestTimestamp) {
        return;
      } else {
        this.latestTimestamp = newTimestamp;
      }
    } else {
      return;
    }
    final List<Window> removableWindows = new LinkedList<>();
    for (final Window window : this.windowToState.keySet()) {
      if (window.getMaxTimestamp() < this.latestTimestamp) {
        final Map<KEY, STATE> keyToState = this.windowToState.get(window);
        for (final STATE state : keyToState.values()) {
          this.emitEvent(this.aggregator.getResult(state));
        }
        removableWindows.add(window);
      }
    }
    for (final Window removableWindow : removableWindows) {
      this.windowToState.remove(removableWindow);
    }
  }

  @Override
  public void process(@NotNull final AData<IN> input) {
    this.processWatermark(input);

    final List<Window> activeWindows = this.windowAssigner.assignWindows(input.getEventTime());
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

  private void emitEvent(final OUT event) {
    final long timestamp = this.timestampExtractor.apply(event);
    final AData<OUT> wrappedEvent = new AData<>(event, timestamp, false);
    final AData<OUT> watermarkedEvent = this.watermarkGenerator.apply(wrappedEvent);
    this.collector.collect(watermarkedEvent);
  }

}
