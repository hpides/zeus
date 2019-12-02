package de.hpi.des.hdes.benchmark;

import lombok.Value;

@Value
public class TupleEvent implements Event {

  private final long key;
  private final int value;
  private final boolean isBenchmarkCheckpoint;

  @Override
  public long getKey() {
    return key;
  }
}
