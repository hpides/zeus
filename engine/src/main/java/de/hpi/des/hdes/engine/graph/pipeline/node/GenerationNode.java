package de.hpi.des.hdes.engine.graph.pipeline.node;

import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.generators.Generatable;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import lombok.Getter;

@Getter
public abstract class GenerationNode extends Node {
    final protected Generatable operator;
    final protected PrimitiveType[] inputTypes;
    final protected PrimitiveType[] outputTypes;

    protected GenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes,
            Generatable operator) {
        super();
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.operator = operator;
    }

    protected GenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes, Generatable operator,
            String string) {
        super(string);
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.operator = operator;
    }

    public abstract void accept(PipelineTopology pipelineTopology);

    @Override
    public String getNodeId() {
        String prefix = this.getParents().stream().map(n -> n.getNodeId()).collect(Collectors.joining());
        return operator == null ? prefix : operator.getOperatorId().concat(prefix);
    }

    @Override
    public int hashCode() {
        return this.getNodeId().hashCode();
    }

}