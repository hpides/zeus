package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import lombok.Getter;

@Getter
public abstract class GenerationNode extends Node {
    final protected PrimitiveType[] inputTypes;
    final protected PrimitiveType[] outputTypes;

    protected GenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes) {
        super();
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
    }

    protected GenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] outputTypes, String string) {
        super(string);
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
    }

    public abstract void accept(PipelineTopology pipelineTopology);

}