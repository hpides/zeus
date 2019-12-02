package de.hpi.des.hdes.engine.window;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class TimeWindow implements Window {

  private final long start;
  private final long end;

  public TimeWindow(final long start, final long end) {
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return this.start;
  }


  public long getEnd() {
    return this.end;
  }

  @Override
  public long getMaxTimestamp() {
    return this.end - 1;
  }

}
