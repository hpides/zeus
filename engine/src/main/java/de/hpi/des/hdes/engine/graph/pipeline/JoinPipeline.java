package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

@Getter
public class JoinPipeline extends BinaryPipeline {

    protected JoinPipeline(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    protected JoinPipeline(Node binaryNode) {
        super(binaryNode);
    }

    public static JoinPipeline of(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        return new JoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public JoinGenerationNode getBinaryNode() {
        return (JoinGenerationNode) this.binaryNode;
    }
}