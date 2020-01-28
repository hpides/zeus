package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;

public interface TwoInputOperator<IN1, IN2, OUT> extends Operator<OUT> {

  void processStream1(AData<IN1> in);

  void processStream2(AData<IN2> in);
}
