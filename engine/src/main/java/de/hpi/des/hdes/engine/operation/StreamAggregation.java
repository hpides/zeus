package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Operator that aggregates a Stream
 *
 * @param <IN>    the type of the input elements
 * @param <STATE> the type of the state elements
 * @param <OUT>   the type of the output elements
 */
public class StreamAggregation<IN, STATE, OUT> extends AbstractTopologyElement<OUT> implements
    OneInputOperator<IN, OUT> {

  private final Aggregator<IN, STATE, OUT> aggregator;
  private final WindowAssigner<? extends Window> windowAssigner;
  private final WatermarkGenerator<OUT> watermarkGenerator;
  private final TimestampExtractor<OUT> timestampExtractor;
  private final HashMap<Window, STATE> windowToState;
  private long latestTimestamp = 0;

  public StreamAggregation(final Aggregator<IN, STATE, OUT> aggregator,
      final WindowAssigner<? extends Window> windowAssigner,
      final WatermarkGenerator<OUT> watermarkGenerator,
      final TimestampExtractor<OUT> timestampExtractor) {
    this.aggregator = aggregator;
    this.windowAssigner = windowAssigner;
    this.windowToState = new HashMap<>();
    this.watermarkGenerator = watermarkGenerator;
    this.timestampExtractor = timestampExtractor;
    aggregator.initialize();
  }

  @Override
  public void process(final AData<IN> in) {
    final List<? extends Window> activeWindows = this.windowAssigner
        .assignWindows(in.getEventTime());
    final IN input = in.getValue();
    // Add the input to the windows it belongs to
    for (final Window window : activeWindows) {
      final STATE state = this.windowToState
          .computeIfAbsent(window, w -> this.aggregator.initialize());
      this.windowToState.put(window, this.aggregator.add(state, input));
    }
    this.processWatermark(in);
  }

  private void processWatermark(final AData<IN> in) {
    if (in.isWatermark()) {
      final long newTimestamp = ((ADataWatermark) (in)).getWatermarkTimestamp();
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
        final STATE state = this.windowToState.get(window);
        this.emitEvent(this.aggregator.getResult(state));
        removableWindows.add(window);
      }
    }
    for (final Window window : removableWindows) {
      this.windowToState.remove(window);
    }
  }

  private void emitEvent(final OUT event) {
    final long timestamp = this.timestampExtractor.apply(event);
    final AData<OUT> wrappedEvent = new AData<>(event, timestamp, false);
    final AData<OUT> watermarkedEvent = this.watermarkGenerator.apply(wrappedEvent);
    this.collector.collect(watermarkedEvent);
  }
}
