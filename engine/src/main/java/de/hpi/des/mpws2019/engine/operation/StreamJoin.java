package de.hpi.des.mpws2019.engine.operation;

import de.hpi.des.mpws2019.engine.udf.Join;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class StreamJoin<IN1, IN2, OUT> extends AbstractInitializable<OUT> implements
    TwoInputOperator<IN1, IN2, OUT> {

  private final Join<IN1, IN2, OUT> join;
  private final BiPredicate<IN1, IN2> joinCondition;
  private final List<IN1> state1 = new ArrayList<>();
  private final List<IN2> state2 = new ArrayList<>();

  public StreamJoin(final Join<IN1, IN2, OUT> join,
                    final BiPredicate<IN1, IN2> joinCondition) {
    this.join = join;
    this.joinCondition = joinCondition;
  }

  @Override
  public void processStream1(final IN1 in) {
    this.state1.add(in);
    for (final IN2 element : this.state2) {
      if (this.joinCondition.test(in, element)) {
        final OUT result = this.join.join(in, element);
        this.collector.collect(result);
      }
    }
  }

  @Override
  public void processStream2(final IN2 in) {
    this.state2.add(in);
    for (final IN1 element : this.state1) {
      if (this.joinCondition.test(element, in)) {
        final OUT result = this.join.join(element, in);
        this.collector.collect(result);
      }
    }
  }
}
