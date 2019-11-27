package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.NodeVisitor;
import lombok.extern.slf4j.Slf4j;

/**
 * This node is just used for it's UUID generated in the Node class.
 */
public class DummyNode extends Node {
  @Override
  public void accept(NodeVisitor visitor) {

  }
}
