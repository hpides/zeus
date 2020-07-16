package de.hpi.des.hdes.engine;

import java.util.LinkedList;
import java.util.List;

import de.hpi.des.hdes.engine.execution.Stoppable;
import de.hpi.des.hdes.engine.execution.plan.CompiledExecutionPlan;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompiledEngine implements Engine {

    @Getter
    private CompiledExecutionPlan plan;
    @Getter
    private boolean isRunning;
    private final List<Stoppable> runningPiplines = new LinkedList<>();

    public CompiledEngine() {
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
        for (final Pipeline pipeline : this.plan.getRunnablePiplines()) {
            log.info("Pipeline {} submitted", pipeline);
            Thread t = new Thread((Runnable) pipeline.getPipelineObject());
            t.start();
            runningPiplines.add((Stoppable) pipeline.getPipelineObject());
        }

    }

    @Override
    public void addQuery(Query query) {
        this.plan = CompiledExecutionPlan.extend(this.plan, query);
    }

    @Override
    public void deleteQuery(Query query) {
        // TODO engine: what about Pipelines with shared operators?
        this.plan = CompiledExecutionPlan.delete(this.plan, query);
    }

    @Override
    public void shutdown() {
        for (Stoppable t : runningPiplines) {
            t.shutdown();
        }
        runningPiplines.clear();
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}