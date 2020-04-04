package de.hpi.des.hdes.engine.window;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A basic window with start end non inclusive end time.
 */
@EqualsAndHashCode
@ToString
public class TimeWindow implements Window {

  private final long start;
  private final long end;

  /**
   * @param start Window start time
   * @param end Window end time
   */
  public TimeWindow(final long start, final long end) {
    this.start = start;
    this.end = end;
  }

  /**
   *
   * @return Window start time.
   */
  public long getStart() {
    return this.start;
  }


  /**
   *
   * @return Window end time.
   */
  public long getEnd() {
    return this.end;
  }

  @Override
  public long getMaxTimestamp() {
    return this.end - 1;
  }

}
