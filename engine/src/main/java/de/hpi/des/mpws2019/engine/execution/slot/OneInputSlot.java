package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.OneInputOperator;
import de.hpi.des.mpws2019.engine.operation.Collector;

public class OneInputSlot<IN, OUT> extends Slot {

  private final OneInputOperator<IN, OUT> operator;
  private final InputBuffer<IN> input;

  public OneInputSlot(final OneInputOperator<IN, OUT> operator, final InputBuffer<IN> input,
                      final Collector<OUT> output) {
    this.operator = operator;
    this.input = input;
    this.operator.init(output);
  }

  public void run() {
    final IN in = this.input.poll();
    if (in != null) {
      this.operator.process(in);
    }
  }
}
