package de.hpi.des.hdes.engine.operation;

public interface Sink<IN> {

  void process(IN in);
}

