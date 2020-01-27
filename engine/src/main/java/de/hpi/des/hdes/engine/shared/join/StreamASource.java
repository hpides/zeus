package de.hpi.des.hdes.engine.shared.join;

import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jooq.lambda.Seq;

@Slf4j
public class StreamASource<IN, KEY> extends AbstractTopologyElement<Bucket<KEY, IN>>
    implements OneInputOperator<IN, Bucket<KEY, IN>> {

  private final int triggerInterval;
  private final WindowAssigner<? extends Window> windowAssigner;
  private final KeySelector<IN, KEY> keySelector;
  private final Map<Window, Set<IN>> state;
  private final Timer timer;

  public StreamASource(final int triggerInterval,
                       final WindowAssigner<? extends Window> windowAssigner,
                       final KeySelector<IN, KEY> keySelector) {
    this.triggerInterval = triggerInterval;
    this.windowAssigner = windowAssigner;
    this.keySelector = keySelector;
    this.state = new HashMap<>();
    this.timer = new Timer();
  }
  // 1. pull from external source
  // 2. combine entries of last t time slots into a bucket

  @Override
  public void init(final Collector<Bucket<KEY, IN>> collector) {
    super.init(collector);
    this.timer.scheduleAtFixedRate(new TriggerTask(), this.triggerInterval, this.triggerInterval);
  }

  @Override
  public void process(@NotNull final IN in) {
    final List<? extends Window> assignedWindows = this.windowAssigner
        .assignWindows(System.currentTimeMillis());
    for (final Window window : assignedWindows) {
      // put in own state
      final Set<IN> ownState = this.state.computeIfAbsent(window, w -> new HashSet<>());
      ownState.add(in);
    }
  }

  private void trigger() {
    final List<? extends Window> currentWindows = this.windowAssigner
        .assignWindows(System.currentTimeMillis());
    for (final Entry<Window, Set<IN>> entry : this.state.entrySet()) {
      final Window window = entry.getKey();
      // emit all closed windows
      if (!currentWindows.contains(window)) {
        final Map<KEY, Set<IN>> indices = Seq.seq(entry.getValue())
            .groupBy(this.keySelector::selectKey, Collectors.toSet());
        this.collector.collect(new Bucket<>(indices, window));
        this.state.remove(window);
      }
    }
  }

  private class TriggerTask extends TimerTask {

    @Override
    public void run() {
      StreamASource.this.trigger();
    }
  }
}
