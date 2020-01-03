package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.slot.Slot;
import java.util.List;

public class TestUtil {

  public static void stepSleepAndTick(Slot slot)  {
    slot.runStep();
    slot.tick();

  }

  public static void stepSleepAndTick(List<Slot> slots)  {
    slots.forEach(TestUtil::stepSleepAndTick);
  }
}
