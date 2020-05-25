package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;

import lombok.Getter;

@Getter
public class PipelineTopology {

    private final List<Pipeline> pipelines;

    public PipelineTopology(final List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }

    public List<RunnablePipeline> getRunnablePiplines() {
        // TODO engine Return all objects that implement RunnablePipeline (source
        // pipelines and buffered sources)
        return null;
    }
}
