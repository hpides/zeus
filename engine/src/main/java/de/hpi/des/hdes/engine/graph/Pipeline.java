package de.hpi.des.hdes.engine.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

@Getter
public class Pipeline {

    private final Set<Node> nodes;
    private final String pipelineId;
    private final List<Pipeline> parents = new ArrayList<>();
    @Setter
    private Pipeline child;

    protected Pipeline(final Set<Node> nodes) {
        this.nodes = nodes;
        this.pipelineId = "c".concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public static Pipeline of(final Set<Node> nodes) {
        return new Pipeline(nodes);
    }

    void addParent(Pipeline pipeline) {
        this.parents.add(pipeline);
        pipeline.setChild(this);
    }

    void loadPipeline() {

    }
}
