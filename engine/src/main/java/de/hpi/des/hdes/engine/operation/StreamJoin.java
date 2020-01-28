package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class StreamJoin<IN1, IN2, OUT, W extends Window> extends AbstractTopologyElement<OUT> implements
    TwoInputOperator<IN1, IN2, OUT> {

  private final Join<IN1, IN2, OUT> join;
  private final BiPredicate<IN1, IN2> joinCondition;
  private final WindowAssigner<W> windowAssigner;
  private final Map<Window, List<IN1>> state1 = new HashMap<>();
  private final Map<Window, List<IN2>> state2 = new HashMap<>();

  public StreamJoin(final Join<IN1, IN2, OUT> join,
                    final BiPredicate<IN1, IN2> joinCondition,
                    final WindowAssigner<W> windowAssigner) {
    this.join = join;
    this.joinCondition = joinCondition;
    this.windowAssigner = windowAssigner;
  }

  @Override
  public void processStream1(final AData<IN1> in) {
    // TODO: change interface to AData<IN1> and pass its timestamp
    final List<? extends W> assignedWindows = this.windowAssigner.assignWindows(System.nanoTime());
    for (final Window window : assignedWindows) {
      // put in own state
      final List<IN1> ownState = this.state1.computeIfAbsent(window, w -> new ArrayList<>());
      ownState.add(in.getValue());

      // join
      for (final IN2 element : this.state2.getOrDefault(window, Collections.emptyList())) {
        if (this.joinCondition.test(in.getValue(), element)) {
          final OUT result = this.join.join(in.getValue(), element);
          this.collector.collect(AData.of(result));
        }
      }
    }
  }

  @Override
  public void processStream2(final AData<IN2> in) {
    // TODO: change interface to AData<IN2> and pass its timestamp
    final List<? extends W> assignedWindows = this.windowAssigner.assignWindows(System.nanoTime());
    for (final Window window : assignedWindows) {
      // put in own state
      final List<IN2> ownState = this.state2.computeIfAbsent(window, w -> new ArrayList<>());
      ownState.add(in.getValue());

      // join
      for (final IN1 element : this.state1.getOrDefault(window, Collections.emptyList())) {
        if (this.joinCondition.test(element, in.getValue())) {
          final OUT result = this.join.join(element, in.getValue());
          this.collector.collect(AData.of(result));
        }
      }
    }
  }
}
