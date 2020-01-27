package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import java.util.List;

public class TestUtil {

  public static void runAndTick(final RunnableSlot<?> slot) {
    slot.runStep();
    slot.tick();

  }

  public static void runAndTick(final List<RunnableSlot<?>> slots) {
    slots.forEach(TestUtil::runAndTick);
  }
}
