package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.SourceNode;
import de.hpi.des.hdes.engine.operation.Source;

public class SourceSlot<OUT> extends VulcanoRunnableSlot<OUT> {

  private final Source<OUT> source;
  private final SourceNode<OUT> topologyNode;

  public SourceSlot(final Source<OUT> source, final SourceNode<OUT> topologyNode) {
    this.source = source;
    this.topologyNode = topologyNode;
  }

  @Override
  public void runStep() {
    this.source.read();
  }

  @Override
  public SourceNode<OUT> getTopologyNode() {
    return this.topologyNode;
  }

}
