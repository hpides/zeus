package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.operation.Collector;

public class TwoInputSlot<IN1, IN2, OUT> extends Slot {

  private final TwoInputOperator<IN1, IN2, OUT> operator;
  private final Buffer<IN1> input1;
  private final Buffer<IN2> input2;


  public TwoInputSlot(final TwoInputOperator<IN1, IN2, OUT> operator, final Buffer<IN1> input1,
      final Buffer<IN2> input2, final Collector<OUT> output) {
    this.operator = operator;
    this.input1 = input1;
    this.input2 = input2;
  }

  public void runStep() {
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