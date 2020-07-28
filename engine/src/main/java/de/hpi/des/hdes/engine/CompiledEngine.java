package de.hpi.des.hdes.engine;

import java.util.LinkedList;
import java.util.List;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.Stoppable;
import de.hpi.des.hdes.engine.generators.LocalGenerator;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompiledEngine implements Engine {

    private final PipelineTopology pipelineTopology = new PipelineTopology();
    private final LocalGenerator generator = new LocalGenerator();
    private final Dispatcher dispatcher = new Dispatcher();
    private final List<Stoppable> runningPiplines = new LinkedList<>();

    @Override
    public void addQuery(final Query query) {
        PipelineTopology newPipelineTopology = PipelineTopology.pipelineTopologyOf(query);
        List<Pipeline> newPipelines = this.pipelineTopology.extend(newPipelineTopology);

        generator.extend(newPipelines);
        dispatcher.extend(newPipelines);

        loadPipelines(newPipelines);
        for (final Pipeline pipeline : newPipelines) {
            log.info("Pipeline {} submitted", pipeline);
            Thread t = new Thread((Runnable) pipeline.getPipelineObject());
            t.start();
            runningPiplines.add((Stoppable) pipeline.getPipelineObject());
        }
    }

    private void loadPipelines(final List<Pipeline> pipelines) {
        for (final Pipeline pipeline : pipelines) {
            pipeline.loadPipeline(dispatcher, null);
        }
    }

    @Override
    public void deleteQuery(Query query) {
        // TODO engine: what about Pipelines with shared operators?
    }

    @Override
    public void shutdown() {
        for (Stoppable t : runningPiplines) {
            t.shutdown();
        }
        runningPiplines.clear();
    }
}