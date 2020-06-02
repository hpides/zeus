package de.hpi.des.hdes.engine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import de.hpi.des.hdes.engine.execution.plan.CompiledExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.CompiledRunnableSlot;
import de.hpi.des.hdes.engine.graph.pipeline.RunnablePipeline;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompiledEngine implements Engine {

    @Getter
    private CompiledExecutionPlan plan;
    private final ExecutorService executor;
    @Getter
    private boolean isRunning;

    public CompiledEngine() {
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
        for (final Runnable pipeline : this.plan.getRunnablePiplines()) {
            log.debug("Pipeline {} submitted", pipeline);
            this.executor.submit(pipeline);
        }

    }

    @Override
    public void addQuery(Query query) {
        Topology topology = query.getTopology();
        this.plan = CompiledExecutionPlan.extend(this.plan, topology);

        for (final Runnable pipeline : this.plan.getRunnablePiplines()) {
            log.debug("Pipeline {} submitted", pipeline);
            this.executor.submit(pipeline);
        }

        // TODO keep track of running pipeline

    }

    @Override
    public void deleteQuery(Query query) {
        // TODO engine: what about Pipelines with shared operators?
        List<RunnablePipeline> pipelinesToShutdown = this.plan.getRunnablePiplinesFor(query);
        this.plan = CompiledExecutionPlan.delete(this.plan, query);
        pipelinesToShutdown.forEach(RunnablePipeline::shutdown);
    }

    @Override
    public void shutdown() {
        this.plan.getRunnablePiplines().forEach(RunnablePipeline::shutdown);

        this.executor.shutdownNow();
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}