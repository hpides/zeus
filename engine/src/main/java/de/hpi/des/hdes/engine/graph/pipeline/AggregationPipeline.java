package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.node.AggregationGenerationNode;
import lombok.Getter;

@Getter
public class AggregationPipeline extends UnaryPipeline {

    AggregationGenerationNode aggregationGenerationNode;

    public AggregationPipeline(AggregationGenerationNode aggregationGenerationNode) {
        super(aggregationGenerationNode);
        this.aggregationGenerationNode = aggregationGenerationNode;
	}

	@Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

}