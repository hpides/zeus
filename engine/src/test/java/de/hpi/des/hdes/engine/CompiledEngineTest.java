package de.hpi.des.hdes.engine;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.execution.plan.CompiledExecutionPlan;
import de.hpi.des.hdes.engine.execution.plan.Pipeline;
import de.hpi.des.hdes.engine.execution.slot.CompiledRunnableSlot;

public class CompiledEngineTest {

    // @Test
    public void testQuery() throws InterruptedException {
        CompiledEngine engine = new CompiledEngine();
        List<CompiledRunnableSlot> slots = engine.getPlan().getSlots();
        Pipeline pipeline = new Pipeline("/Users/nils/Documents/MP/HDES/engine/TestQuery.java", "TestQuery");
        slots.add(new CompiledRunnableSlot(pipeline, null));
        engine.run();
        TimeUnit.MILLISECONDS.sleep(1);
        engine.shutdown();
    }
}