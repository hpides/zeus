package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.Source;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TopologyBuilder {

  private Node latest;

  public void addGraphNode(final Node parent, final Node child) {
    parent.addChild(child);
    this.latest = child;
  }

  public <V> AStream<V> streamOf(final Source<V> source) {
    final SourceNode<V> sourceNode = new SourceNode<>(source);
    this.latest = sourceNode;
    return new AStream<>(this, sourceNode);
  }

  public Topology build() {
    final List<SourceNode<?>> collect = getParents(this.latest)
        .filter(node -> node instanceof SourceNode)
        .map(node -> (SourceNode<?>) node)
        .collect(Collectors.toList());
    return new Topology(collect);
  }

  private static Stream<Node> getParents(final Node node) {
    return Stream
        .concat(Stream.of(node), node.getParents().stream().flatMap(TopologyBuilder::getParents));
  }
}
