package de.hpi.des.hdes.engine.execution;

import de.hpi.des.hdes.engine.AData;

public interface SlotProcessor<IN> {

  void sendDownstream(IN event);
}
