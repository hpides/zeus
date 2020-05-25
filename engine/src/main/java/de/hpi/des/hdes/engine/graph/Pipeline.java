package de.hpi.des.hdes.engine.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class Pipeline {

    private final String pipelineId;
    private final List<Pipeline> parents = new ArrayList<>();
    @Setter
    private Pipeline child;

    protected Pipeline() {
        this.pipelineId = "c".concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    void addParent(Pipeline pipeline) {
        this.parents.add(pipeline);
        pipeline.setChild(this);
    }

    public abstract void accept(PipelineVisitor visitor);

    void loadPipeline() {

    }
}
