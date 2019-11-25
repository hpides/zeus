package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.OneInputOperator;
import de.hpi.des.mpws2019.engine.operation.Output;

public class OneInputSlot<In, Out> extends Slot {

  private final OneInputOperator<In, Out> operator;
  private final Input<In> input;

  public OneInputSlot(final OneInputOperator<In, Out> operator, final Input<In> input,
                      final Output<Out> output) {
    this.operator = operator;
    this.input = input;
    this.operator.init(output);
  }

  public void run() {
    final var in = this.input.poll();
    if (in != null) {
      this.operator.process(in);
    }
  }
}
