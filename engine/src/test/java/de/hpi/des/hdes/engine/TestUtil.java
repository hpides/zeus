package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.slot.VulcanoRunnableSlot;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;

import java.util.List;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtil {

  public static void runAndTick(final VulcanoRunnableSlot<?> slot) {
    slot.runStep();
    slot.tick();

  }

  public static void runAndTick(final List<VulcanoRunnableSlot<?>> slots) {
    slots.forEach(TestUtil::runAndTick);
  }

  public static boolean contains(PipelineTopology pt, String substring){
    return pt.getPipelines().stream().anyMatch(pipeline -> {
      try {
        String result = Files.readString(Paths.get(pipeline.getPipelineId() + ".java"));
        return result.replaceAll("\\s{2,}", "").contains(substring);
      } catch (IOException e) {
        return false;
      }
    });
  }
}
