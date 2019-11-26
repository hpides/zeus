package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.BinaryOperator;
import de.hpi.des.mpws2019.engine.operation.Collector;

public class TwoInputSlot<In1, In2, Out> extends Slot {

  private final BinaryOperator<In1, In2, Out> operator;
  private final InputBuffer<In1> input1;
  private final InputBuffer<In2> input2;


  public TwoInputSlot(final BinaryOperator<In1, In2, Out> operator, final InputBuffer<In1> input1,
                      final InputBuffer<In2> input2, final Collector<Out> output) {
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