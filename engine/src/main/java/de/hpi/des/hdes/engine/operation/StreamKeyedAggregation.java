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

  public void processWatermark(AData<IN> in) {
    if (in.isWatermark()) {
      long newTimestamp = ((ADataWatermark)(in)).getWatermarkTimestamp();
      if (newTimestamp <= this.latestTimestamp) {
        return;
      }
      else this.latestTimestamp = newTimestamp;
    } else {
      return;
    }
    List<Window> removableWindows = new LinkedList<>();
    for (final Window window : this.windowToState.keySet()) {
      if (window.getMaxTimestamp() < this.latestTimestamp) {
        final Map<KEY, STATE> keyToState = this.windowToState.get(window);
        for (final STATE state : keyToState.values()) {
          emitEvent(this.aggregator.getResult(state));
        }
        removableWindows.add(window);
      }
    }
    for (Window removableWindow : removableWindows) {
      this.windowToState.remove(removableWindow);
    }
  }

  @Override
  public void process(@NotNull final AData<IN> input) {
    processWatermark(input);

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

  private void emitEvent(OUT event) {
    long timestamp = timestampExtractor.apply(event);
    AData<OUT> wrappedEvent = new AData<>(event, timestamp, false);
    AData<OUT> watermarkedEvent = watermarkGenerator.apply(wrappedEvent);
    this.collector.collect(watermarkedEvent);
  }
}
