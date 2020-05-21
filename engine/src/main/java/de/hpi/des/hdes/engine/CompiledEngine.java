package de.hpi.des.hdes.engine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.hpi.des.hdes.engine.execution.plan.CompiledExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.CompiledRunnableSlot;
import de.hpi.des.hdes.engine.graph.Topology;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompiledEngine implements Engine {

    @Getter
    private CompiledExecutionPlan plan;
    private final ExecutorService executor;
    @Getter
    private boolean isRunning;

    CompiledEngine() {
        this.executor = Executors.newCachedThreadPool();
        this.plan = CompiledExecutionPlan.emptyExecutionPlan();
        this.isRunning = false;
    }

    @Override
    public void run() {
        log.info("Starting Engine");
        if (this.isRunning) {
            throw new IllegalStateException("Engine already running");
        }

        this.isRunning = true;
        for (final CompiledRunnableSlot slot : this.plan.getRunnableSlots()) {
            log.debug("Slot {} submitted", slot);
            this.executor.submit(slot);
        }

    }

    @Override
    public void addQuery(Query query) {
        Topology topology = query.getTopology();
        this.plan = CompiledExecutionPlan.extend(this.plan, topology);

        for (CompiledRunnableSlot slot : this.plan.getSlots()) {
            this.executor.submit(slot);
        }

        // TODO keep track of running slots

    }

    @Override
    public void deleteQuery(Query query) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        this.plan.getRunnableSlots().forEach(CompiledRunnableSlot::shutdown);

        this.executor.shutdownNow();
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}