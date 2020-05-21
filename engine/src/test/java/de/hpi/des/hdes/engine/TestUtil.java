package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.slot.VulcanoRunnableSlot;
import java.util.List;

public class TestUtil {

  public static void runAndTick(final VulcanoRunnableSlot<?> slot) {
    slot.runStep();
    slot.tick();

  }

  public static void runAndTick(final List<VulcanoRunnableSlot<?>> slots) {
    slots.forEach(TestUtil::runAndTick);
  }
}
