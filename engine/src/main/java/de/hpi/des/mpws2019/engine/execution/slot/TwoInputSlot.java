package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.BinaryOperator;
import de.hpi.des.mpws2019.engine.operation.Output;

public class TwoInputSlot<In1, In2, Out> implements Slot {

  private final BinaryOperator<In1, In2, Out> operator;
  private final Input<In1> input1;
  private final Input<In2> input2;


  public TwoInputSlot(final BinaryOperator<In1, In2, Out> operator, final Input<In1> input1,
                      final Input<In2> input2, final Output<Out> output) {
    this.operator = operator;
    this.input1 = input1;
    this.input2 = input2;
    operator.init(output);
  }

  public void run() {
    final var in1 = this.input1.poll();
    if (in1 != null) {
      this.operator.processStream1(in1);
    }
    final var in2 = this.input2.poll();
    if (in2 != null) {
      this.operator.processStream2(in2);
    }
  }


}