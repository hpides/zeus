package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.execution.connector.Buffer;
import de.hpi.des.mpws2019.engine.operation.Collector;
import de.hpi.des.mpws2019.engine.operation.OneInputOperator;

public class OneInputSlot<IN, OUT> extends Slot {

  private final OneInputOperator<IN, OUT> operator;
  private final Buffer<IN> input;

  public OneInputSlot(final OneInputOperator<IN, OUT> operator, final Buffer<IN> input,
                      final Collector<OUT> output) {
    this.operator = operator;
    this.input = input;
    this.operator.init(output);
  }

  public void runStep() {
    final IN in = this.input.poll();
    if (in != null) {
      this.operator.process(in);
    }
  }
}
