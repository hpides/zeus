package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.SlotProcessor;

/**
 * A sink writes out incoming elements
 *
 * @param <IN> type of incoming elements
 */
public interface Sink<IN> extends SlotProcessor<AData<IN>> {

  void process(AData<IN> in);

  @Override
  default void sendDownstream(AData<IN> event) {
    this.process(event);
  }
}

