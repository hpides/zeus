package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.predefined.ByteBufferIntListSinkNode;
import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import de.hpi.des.hdes.engine.generators.Generatable;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.UnaryGeneratable;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;

/**
 * Represents a unary operation in the logical plan.
 *
 * @param <IN>  type of incoming elements
 * @param <OUT> type of outgoing elements
 */
public class UnaryGenerationNode extends GenerationNode {

    private final UnaryGeneratable operator;

    public UnaryGenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes,
            final UnaryGeneratable operator) {
        super(inputTypes, outputTypes, operator);
        this.operator = operator;
    }

    protected UnaryGenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes,
            final String identifier, final UnaryGeneratable operator) {
        super(inputTypes, outputTypes, operator, identifier);
        this.operator = operator;
    }

    public Generatable getOperator() {
        return this.operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        if (this.getChildren().isEmpty()) {
            pipelineTopology.addPipelineAsLeaf(new UnaryPipeline(this), this);
        } else if (this.getOperator() instanceof AggregateGenerator
                || this.getChild() instanceof ByteBufferIntListSinkNode || this.getChild() instanceof FileSinkNode) {
            pipelineTopology.addPipelineAsParent(new UnaryPipeline(this), this);
        } else {
            pipelineTopology.addNodeToPipeline(this);
        }
    }
}
