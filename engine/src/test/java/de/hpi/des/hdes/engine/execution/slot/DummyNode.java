package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;

/**
 * This node is just used for it's UUID generated in the Node class.
 */
public class DummyNode extends Node {
  @Override
  public void accept(NodeVisitor visitor) {

  }
}
