package de.hpi.des.hdes.engine.window.assigner;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.window.TimeWindow;
import java.util.List;
import org.junit.jupiter.api.Test;

class SlidingWindowTest {

  @Test
  void testEventSlidingWindow() {
    final SlidingEventWindow assigner = SlidingWindow.ofEventTime(10, 5);

    final List<TimeWindow> timeWindows = assigner.assignWindows(25L);
    assertThat(timeWindows).containsExactlyInAnyOrder(new TimeWindow(20, 30), new TimeWindow(25, 35));
  }

  @Test
  void testProcessingSlidingWindow() {
    final SlidingEventWindow assigner = SlidingWindow.ofEventTime(10, 5);

    final List<TimeWindow> timeWindows = assigner.assignWindows(25L);
    // just check if it does not use the argument. testEventSlidingWindow tests the same logic
    // deterministically
    assertThat(timeWindows).doesNotContainSequence(new TimeWindow(20, 30), new TimeWindow(25, 35));
  }
}