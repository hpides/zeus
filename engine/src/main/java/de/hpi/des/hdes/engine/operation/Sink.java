package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;

public interface Sink<IN> {

  void process(AData<IN> in);
}

