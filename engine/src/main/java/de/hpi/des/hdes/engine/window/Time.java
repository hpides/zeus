package de.hpi.des.hdes.engine.window;

import java.util.concurrent.TimeUnit;

/**
 * Utility class to convert time units.
 */
public final class Time {

  private final long millis;

  private Time(final long millis) {
    this.millis = millis;
  }

  /**
   * Factory method
   *
   * @param time duration
   * @param unit unit for the duration
   * @return time representation
   */
  public static Time of(final long time, final TimeUnit unit) {
    return new Time(unit.toMillis(time));
  }

  /**
   * Factory method
   *
   * @param millis duration in milliseconds
   * @return time representation
   */
  public static Time of(final long millis) {
    return new Time(millis);
  }

  /**
   * @see Time#of(long)
   */
  public static Time seconds(final long seconds) {
    return new Time(TimeUnit.SECONDS.toMillis(seconds));
  }

  /**
   * @see Time#of(long)
   */
  public static Time minutes(final long minutes) {
    return new Time(TimeUnit.MINUTES.toMillis(minutes));
  }

  /**
   * @see Time#of(long)
   */
  public static Time hours(final long hours) {
    return new Time(TimeUnit.HOURS.toMillis(hours));
  }

  /**
   * Returns the time in milliseconds.
   *
   * @return time in miliseconds
   */
  public long getMillis() {
    return this.millis;
  }

  /**
   * @see Time#getMillis()
   */
  public long getNanos() {
    return this.millis * (long) 1e6;
  }
}
