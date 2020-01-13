package de.hpi.des.hdes.engine.window;

import java.util.concurrent.TimeUnit;

public final class Time {

  private final long millis;

  private Time(final long millis) {
    this.millis = millis;
  }

  public static Time of(final long time, final TimeUnit unit) {
    return new Time(unit.toMillis(time));
  }

  public static Time of(final long millis) {
    return new Time(millis);
  }

  public static Time seconds(final long seconds) {
    return new Time(TimeUnit.SECONDS.toMillis(seconds));
  }

  public static Time minutes(final long minutes) {
    return new Time(TimeUnit.MINUTES.toMillis(minutes));
  }

  public static Time hours(final long hours) {
    return new Time(TimeUnit.HOURS.toMillis(hours));
  }

  public long getMillis() {
    return this.millis;
  }

  public long getNanos() {
    return this.millis * (long) 1e6;
  }
}
