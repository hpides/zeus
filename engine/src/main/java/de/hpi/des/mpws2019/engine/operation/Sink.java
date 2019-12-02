package de.hpi.des.mpws2019.engine.operation;

public interface Sink<IN> {

  void process(IN in);
}

