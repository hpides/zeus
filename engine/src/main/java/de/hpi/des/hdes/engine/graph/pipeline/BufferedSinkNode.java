package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;

public class BufferedSinkNode extends GenerationNode {
  @Getter
  private final BufferedSink sink;

  public BufferedSinkNode(BufferedSink sink) {
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
