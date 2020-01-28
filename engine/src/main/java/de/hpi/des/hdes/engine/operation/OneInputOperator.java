package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;

public interface OneInputOperator<IN, OUT> extends Operator<OUT> {

  void process(AData<IN> in);
}
