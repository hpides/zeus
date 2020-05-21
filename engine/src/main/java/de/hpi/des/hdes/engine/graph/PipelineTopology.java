package de.hpi.des.hdes.engine.graph;

import java.util.List;

import lombok.Getter;

@Getter
public class PipelineTopology {

    private final List<Pipeline> pipelines;

    public PipelineTopology(final List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }
}
