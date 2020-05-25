package de.hpi.des.hdes.engine.astream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

/**
 * Base class for all AStream types.
 *
 * An AStream type represents the current state of the stream.
 *
 * @param <In>
 */
public abstract class AbstractAStream<In> {

  protected VulcanoTopologyBuilder builder;
  protected Node node;

  protected AbstractAStream(final VulcanoTopologyBuilder builder, final Node node) {
    this.builder = builder;
    this.node = node;
  }

  public VulcanoTopologyBuilder getBuilder() {
    return this.builder;
  }

  protected Node getNode() {
    return this.node;
  }
}
