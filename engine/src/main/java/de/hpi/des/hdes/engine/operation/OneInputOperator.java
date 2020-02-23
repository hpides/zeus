package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.SlotProcessor;

public interface OneInputOperator<IN, OUT> extends Operator<OUT>, SlotProcessor<AData<IN>> {

  void process(AData<IN> in);

  @Override
  default void sendDownstream(final AData<IN> event) {
    this.process(event);
  }
}
