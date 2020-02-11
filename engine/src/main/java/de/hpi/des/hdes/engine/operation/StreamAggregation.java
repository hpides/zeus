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

public class StreamAggregation<IN, STATE, OUT> extends AbstractTopologyElement<OUT> implements OneInputOperator<IN, OUT> {
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
    final List<? extends Window> activeWindows = this.windowAssigner.assignWindows(in.getEventTime());
    final IN input = in.getValue();
    // Add the input to the windows it belongs to
    for (final Window window : activeWindows) {
      final STATE state = this.windowToState.computeIfAbsent(window, w -> this.aggregator.initialize());
      this.windowToState.put(window, this.aggregator.add(state, input));
    }
    processWatermark(in);
  }

  public void processWatermark(AData<IN> in) {
    if (in.isWatermark()) {
      long newTimestamp = ((ADataWatermark) (in)).getWatermarkTimestamp();
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
        STATE state = this.windowToState.get(window);
        emitEvent(this.aggregator.getResult(state));
        removableWindows.add(window);
      }
    }
    for (Window window : removableWindows) {
      windowToState.remove(window);
    }
  }

  private void emitEvent(OUT event) {
    long timestamp = timestampExtractor.apply(event);
    AData<OUT> wrappedEvent = new AData<>(event, timestamp, false);
    AData<OUT> watermarkedEvent = watermarkGenerator.apply(wrappedEvent);
    this.collector.collect(watermarkedEvent);
  }
}
