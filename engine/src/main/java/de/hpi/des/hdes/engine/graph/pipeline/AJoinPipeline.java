package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

@Getter
public class AJoinPipeline extends Pipeline {

    private final List<Node> leftNodes;
    private final List<Node> rightNodes;
    private final AJoinGenerationNode binaryNode;
    private Pipeline leftParent;
    private Pipeline rightParent;

    protected AJoinPipeline(List<Node> leftNodes, List<Node> rightNodes, AJoinGenerationNode binaryNode) {
        super();
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
        this.binaryNode = binaryNode;
    }

    protected AJoinPipeline(AJoinGenerationNode binaryNode) {
        super();
        this.binaryNode = binaryNode;
        this.leftNodes = new ArrayList<>();
        this.rightNodes = new ArrayList<>();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void addParent(Pipeline pipeline, Node childNode) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addOperator(Node operator, Node childNode) {
        // TODO Auto-generated method stub

    }

}