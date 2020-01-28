package de.hpi.des.hdes.engine.graph;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import java.util.LinkedList;
import java.util.List;

public class TopologyBuilder {

  private final List<Node> nodes = new LinkedList<>();

  public void addGraphNode(final Node parent, final Node child) {
    parent.addChild(child);
    this.nodes.add(child);
  }

  public static TopologyBuilder newQuery() {
    return new TopologyBuilder();
  }

  public <V> AStream<V> streamOf(final Source<V> source) {
    final SourceNode<V> sourceNode = new SourceNode<>(source);
    this.nodes.add(sourceNode);
    return new AStream<>(this, sourceNode);
  }

  public Topology build() {
    return new Topology(Sets.newHashSet(this.nodes));
  }

  public Query buildAsQuery() {
    return new Query(new Topology(Sets.newHashSet(this.nodes)));
  }
}