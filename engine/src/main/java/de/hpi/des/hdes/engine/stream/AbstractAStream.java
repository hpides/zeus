package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;

public abstract class AbstractAStream<In> {

  protected TopologyBuilder builder;
  protected Node node;

  protected AbstractAStream(final TopologyBuilder builder, final Node node) {
    this.builder = builder;
    this.node = node;
  }

  public TopologyBuilder getBuilder() {
    return this.builder;
  }

  protected Node getNode() {
    return this.node;
  }
}
