package de.hpi.des.hdes.engine.operation;

public interface OneInputOperator<IN, OUT> extends Operator<OUT> {

  void process(IN in);
}
