package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.BinaryOperator;
import de.hpi.des.mpws2019.engine.operation.Collector;

public class TwoInputSlot<IN1, IN2, OUT> extends Slot {

  private final BinaryOperator<IN1, IN2, OUT> operator;
  private final InputBuffer<IN1> input1;
  private final InputBuffer<IN2> input2;


  public TwoInputSlot(final BinaryOperator<IN1, IN2, OUT> operator, final InputBuffer<IN1> input1,
                      final InputBuffer<IN2> input2, final Collector<OUT> output) {
    this.operator = operator;
    this.input1 = input1;
    this.input2 = input2;
    operator.init(output);
  }

  public void run() {
    final IN1 in1 = this.input1.poll();
    if (in1 != null) {
      this.operator.processStream1(in1);
    }
    final IN2 in2 = this.input2.poll();
    if (in2 != null) {
      this.operator.processStream2(in2);
    }
  }


}