package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSink;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

public class BufferedSinkNode extends GenerationNode {
  @Getter
  private final BufferedSink sink;

  public BufferedSinkNode(PrimitiveType[] inputTypes, BufferedSink sink) {
      super(inputTypes, null);
      this.sink = sink;
  }

  @Override
  public void accept(NodeVisitor visitor) {
      // TODO Auto-generated method stub

  }

  @Override
  public void accept(PipelineTopology pipelineTopology) {
    SinkPipeline sinkPipeline = new SinkPipeline(this);
    pipelineTopology.addPipelineAsLeaf(sinkPipeline, this);
  }

}
