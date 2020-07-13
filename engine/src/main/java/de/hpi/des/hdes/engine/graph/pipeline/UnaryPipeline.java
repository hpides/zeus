package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

public class UnaryPipeline extends Pipeline {

    @Getter
    private final List<GenerationNode> nodes;
    @Getter
    private Pipeline parent;

    public UnaryPipeline(GenerationNode node) {
        super(node.getInputTypes());
        this.nodes = new ArrayList<GenerationNode>();
        nodes.add(node);
    }

    protected UnaryPipeline(List<GenerationNode> nodes) {
        super(nodes.get(0).getInputTypes());
        this.nodes = nodes;
    }

    public static UnaryPipeline of(final List<GenerationNode> nodes) {
        return new UnaryPipeline(nodes);
    }

    @Override
    public int hashCode() {
        return nodes.stream().mapToInt(t -> t.hashCode()).sum();
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    public boolean hasChild() {
        return this.getChild() != null;
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        this.parent = pipeline;
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        this.nodes.add(operator);
    }
}