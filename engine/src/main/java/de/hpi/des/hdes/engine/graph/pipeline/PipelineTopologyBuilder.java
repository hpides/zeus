package de.hpi.des.hdes.engine.graph;

import com.google.common.collect.Lists;

public class PipelineTopologyBuilder {
    @Getter
    private final List<Node> nodes = new LinkedList<>();


    
    public static PipelineTopology pipelineTopologyOf(Topology queryTopology) {
        // TODO engine
        return new PipelineTopology(Lists.newArrayList());
    }

}
