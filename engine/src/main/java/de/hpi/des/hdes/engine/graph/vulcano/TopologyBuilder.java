package de.hpi.des.hdes.engine.graph;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

/**
 * The topology builder creates a DAG of nodes.
 */
public class TopologyBuilder {

  @Getter
  private final List<Node> nodes = new LinkedList<>();

  /**
   * Adds a new parent-child relationship to the DAG.
   *
   * @param parent the parent node
   * @param child  the child node
   */
  public void addGraphNode(final Node parent, final Node child) {
    parent.addChild(child);
    this.nodes.add(child);
  }

  /**
   * Create a new query with this topology.
   *
   * @return a new builder
   */
  public static TopologyBuilder newQuery() {
    return new TopologyBuilder();
  }

  /**
   * The entry point for the definition of queries in HDES.
   *
   * @param source a source to read data from
   * @param <V>    type of the stream's elements
   * @return a new a stream
   */
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
