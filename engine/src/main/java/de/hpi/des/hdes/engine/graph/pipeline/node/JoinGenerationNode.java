package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.BinaryGeneratable;
import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import java.util.stream.Stream;
import java.util.Arrays;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JoinGenerationNode extends GenerationNode {
    final private PrimitiveType[] joinInputTypes;

    @Getter
    private final JoinGenerator operator;

    public JoinGenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] joinTypes, final BinaryGeneratable operator) {
        super(inputTypes, Stream.concat(Arrays.stream(inputTypes),
            Arrays.stream(joinTypes)).toArray(PrimitiveType[]::new));
        this.joinInputTypes = joinTypes;
        this.operator = (JoinGenerator) operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        JoinPipeline pipeline = new JoinPipeline(this);
        pipelineTopology.addPipelineAsParent(pipeline, this);
    }
}
