package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

public class UnaryPipeline extends Pipeline {

    @Getter
    private final List<Node> nodes;
    @Getter
    private Pipeline parent;

    public UnaryPipeline(Node node) {
        this.nodes = Arrays.asList(node);
    }

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

    public boolean hasChild() {
        return this.getChild() != null;
    }

    @Override
    public void addParent(Pipeline pipeline, Node childNode) {
        this.parent = pipeline;
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(Node operator, Node childNode) {
        this.nodes.add(operator);
    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}
