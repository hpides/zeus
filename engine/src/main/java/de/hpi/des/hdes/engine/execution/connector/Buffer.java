package de.hpi.des.hdes.engine.execution.connector;

public interface Buffer<IN> {

  IN poll();

  void add(IN val);
}
