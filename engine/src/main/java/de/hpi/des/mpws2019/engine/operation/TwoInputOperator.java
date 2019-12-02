package de.hpi.des.mpws2019.engine.operation;

public interface TwoInputOperator<IN1, IN2, OUT> extends Operator<OUT> {

  void processStream1(IN1 in);

  void processStream2(IN2 in);
}
