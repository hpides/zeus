package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.AJoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.AJoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import java.util.stream.Stream;
import java.util.Arrays;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AJoinGenerationNode extends GenerationNode {
    final private PrimitiveType[] joinInputTypes;

    @Getter
    private final AJoinGenerator operator;

    public AJoinGenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] joinTypes, final AJoinGenerator operator) {
        super(inputTypes, Stream.concat(Arrays.stream(inputTypes),
                Arrays.stream(joinTypes)).toArray(PrimitiveType[]::new));
        this.operator = operator;
        this.joinInputTypes = joinTypes;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        AJoinPipeline pipeline = new AJoinPipeline(this);
        pipelineTopology.addPipelineAsParent(pipeline, this);
    }
}