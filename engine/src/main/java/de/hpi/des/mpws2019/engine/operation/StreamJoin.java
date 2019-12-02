package de.hpi.des.mpws2019.engine.operation;

import de.hpi.des.mpws2019.engine.udf.Join;
import de.hpi.des.mpws2019.engine.window.Window;
import de.hpi.des.mpws2019.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class StreamJoin<IN1, IN2, OUT,  W extends Window> extends AbstractInitializable<OUT> implements
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
  public void processStream1(final IN1 in) {
    // TODO: change interface to AData<IN1> and pass its timestamp
    final List<W> assignedWindows = this.windowAssigner.assignWindows(System.currentTimeMillis());
    for (final Window window : assignedWindows) {
      // put in own state
      final List<IN1> ownState = this.state1.computeIfAbsent(window, w -> new ArrayList<>());
      ownState.add(in);

      // join
      for (final IN2 element : this.state2.getOrDefault(window, Collections.emptyList())) {
        if (this.joinCondition.test(in, element)) {
          final OUT result = this.join.join(in, element);
          this.collector.collect(result);
        }
      }
    }
  }

  @Override
  public void processStream2(final IN2 in) {
    // TODO: change interface to AData<IN2> and pass its timestamp
    final List<W> assignedWindows = this.windowAssigner.assignWindows(System.currentTimeMillis());
    for (final Window window : assignedWindows) {
      // put in own state
      final List<IN2> ownState = this.state2.computeIfAbsent(window, w -> new ArrayList<>());
      ownState.add(in);

      // join
      for (final IN1 element : this.state1.getOrDefault(window, Collections.emptyList())) {
        if (this.joinCondition.test(element, in)) {
          final OUT result = this.join.join(element, in);
          this.collector.collect(result);
        }
      }
    }
  }
}
