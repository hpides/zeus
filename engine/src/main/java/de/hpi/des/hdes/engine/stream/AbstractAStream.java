package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;

/**
 * Base class for all AStream types.
 *
 * An AStream type represents the current state of the stream.
 *
 * @param <In>
 */
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
