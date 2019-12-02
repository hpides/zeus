package de.hpi.des.mpws2019.engine.execution.connector;

public interface Buffer<IN> {

  IN poll();

  void add(IN val);
}
