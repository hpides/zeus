package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import lombok.Getter;

public class PipelineTopologyBuilder {
    @Getter
    private final List<Node> nodes = new LinkedList<>();

    public static PipelineTopology pipelineTopologyOf(Topology queryTopology) {
        // TODO engine
        return new PipelineTopology(Lists.newArrayList());
    }

}
