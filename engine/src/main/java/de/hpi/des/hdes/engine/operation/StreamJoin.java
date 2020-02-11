package de.hpi.des.hdes.engine.operation;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class StreamJoin<IN1, IN2, KEY, OUT> extends AbstractTopologyElement<OUT> implements
    TwoInputOperator<IN1, IN2, OUT> {

  private final Join<IN1, IN2, OUT> join;
  private final KeySelector<IN1, KEY> keySelector1;
  private final KeySelector<IN2, KEY> keySelector2;
  private final WindowAssigner<? extends Window> windowAssigner;
  private final WatermarkGenerator<OUT> watermarkGenerator;
  private final TimestampExtractor<OUT> timestampExtractor;
  private final Map<Window, Multimap<KEY, IN1>> state1 = new HashMap<>();
  private final Map<Window, Multimap<KEY, IN2>> state2 = new HashMap<>();
  private long latestWatermark1 = 0;
  private long latestWatermark2 = 0;

  public StreamJoin(final Join<IN1, IN2, OUT> join,
                    final KeySelector<IN1, KEY> keySelector1,
                    final KeySelector<IN2, KEY> keySelector2,
                    final WindowAssigner<? extends Window> windowAssigner,
                    final WatermarkGenerator<OUT> watermarkGenerator,
                    final TimestampExtractor<OUT> timestampExtractor) {
    this.join = join;
    this.keySelector1 = keySelector1;
    this.keySelector2 = keySelector2;
    this.windowAssigner = windowAssigner;
    this.watermarkGenerator = watermarkGenerator;
    this.timestampExtractor = timestampExtractor;
  }

  @Override
  public void processStream1(final AData<IN1> in) {
    final List<? extends Window> assignedWindows = this.windowAssigner.assignWindows(in.getEventTime());
    for (final Window window : assignedWindows) {
      // put in own state
      final Multimap<KEY, IN1> index = this.state1.computeIfAbsent(window, w -> LinkedListMultimap.create());
      index.put(keySelector1.selectKey(in.getValue()), in.getValue());
    }
    processWatermark1(in);
  }

  @Override
  public void processStream2(final AData<IN2> in) {
    final List<? extends Window> assignedWindows = this.windowAssigner.assignWindows(in.getEventTime());
    for (final Window window : assignedWindows) {
      // put in own state
      final Multimap<KEY, IN2> index = this.state2.computeIfAbsent(window, w -> LinkedListMultimap.create());
      index.put(keySelector2.selectKey(in.getValue()), in.getValue());
    }
    processWatermark2(in);
  }

  private void processWatermark1(AData<IN1> event) {
    if (!event.isWatermark()) {
      return;
    }

    ADataWatermark<IN1> watermark = (ADataWatermark<IN1>) event;
    long timestamp = watermark.getWatermarkTimestamp();
    long oldEventTime = Math.min(latestWatermark1, latestWatermark2);
    latestWatermark1 = Math.max(timestamp, latestWatermark1);
    long newEventTime = Math.min(latestWatermark1, latestWatermark2);
    if (newEventTime > oldEventTime) {
      advanceEventTime(Math.min(latestWatermark1, latestWatermark2));
    }
  }

  private void processWatermark2(AData<IN2> event) {
    if (!event.isWatermark()) {
      return;
    }

    ADataWatermark<IN2> watermark = (ADataWatermark<IN2>) event;
    long timestamp = watermark.getWatermarkTimestamp();
    long oldEventTime = Math.min(latestWatermark1, latestWatermark2);
    latestWatermark2 = Math.max(timestamp, latestWatermark2);
    long newEventTime = Math.min(latestWatermark1, latestWatermark2);
    if (newEventTime > oldEventTime) {
      advanceEventTime(Math.min(latestWatermark1, latestWatermark2));
    }
  }

  private void advanceEventTime(long eventTime) {
    Set<Window> windows = Sets.intersection(state1.keySet(),state2.keySet());
    List<Window> sortedWindows = windows.stream()
      .sorted(Comparator.comparingLong(Window::getMaxTimestamp))
      .collect(Collectors.toList());

    for (Window w : sortedWindows) {
      // Window and following windows not finished yet
      if (eventTime < w.getMaxTimestamp()) {
        break;
      }

      Multimap<KEY, IN1> bucket1 = state1.get(w);
      state1.remove(w);
      Multimap<KEY, IN2> bucket2 = state2.get(w);
      state2.remove(w);

      Set<KEY> keyIntersection = Sets.intersection(bucket1.keySet(), bucket2.keySet());
      for (KEY key : keyIntersection) {
        materializeJoin(bucket1.get(key), bucket2.get(key));
      }
    }
  }

  private void materializeJoin(Collection<IN1> keyedValues1, Collection<IN2> keyedValues2) {
    for (IN1 in1 : keyedValues1) {
      for (IN2 in2 : keyedValues2) {
        emitEvent(join.join(in1, in2));
      }
    }
  }

  private void emitEvent(OUT event) {
    long timestamp = timestampExtractor.apply(event);
    AData<OUT> wrappedEvent = new AData<>(event, timestamp, false);
    AData<OUT> watermarkedEvent = watermarkGenerator.apply(wrappedEvent);
    this.collector.collect(watermarkedEvent);
  }
}
