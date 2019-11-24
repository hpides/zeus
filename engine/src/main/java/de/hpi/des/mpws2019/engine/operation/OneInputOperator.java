package de.hpi.des.mpws2019.engine.operation;

public interface OneInputOperator<IN, OUT> extends Operator<OUT> {

  void process(IN in);
}
