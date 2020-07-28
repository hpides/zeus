package de.hpi.des.hdes.engine.window;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CWindow {
  private final long length;
  private final long slide;

  public static CWindow tumblingWindow(Time length) {
    return new CWindow(length.getMillis(), length.getMillis());
  }

  public static CWindow slidingWindow(Time length, Time slide) {
    return new CWindow(length.getMillis(), slide.getMillis());
  }
}