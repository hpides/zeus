package de.hpi.des.hdes.engine.graph;

import java.util.List;

import lombok.Getter;

public class UnaryPipeline extends Pipeline {

    @Getter
    private final List<Node> nodes;

    protected UnaryPipeline(List<Node> nodes) {
        super();
        this.nodes = nodes;
    }

    public static UnaryPipeline of(final List<Node> nodes) {
        return new UnaryPipeline(nodes);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }
}