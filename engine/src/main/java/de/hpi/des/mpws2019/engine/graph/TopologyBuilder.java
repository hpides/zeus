package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.Source;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.LinkedList;
import java.util.List;

public class TopologyBuilder {

  private List<Node> nodes = new LinkedList<>();

  public void addGraphNode(final Node parent, final Node child) {
    parent.addChild(child);
    this.nodes.add(child);
  }

  public <V> AStream<V> streamOf(final Source<V> source) {
    final SourceNode<V> sourceNode = new SourceNode<>(source);
    this.nodes.add(sourceNode);
    return new AStream<>(this, sourceNode);
  }

  public Topology build() {
    return new Topology(nodes);
  }
}
