package de.hpi.des.hdes.benchmark;

public interface Event {

  long getKey();

  boolean isBenchmarkCheckpoint();
}
