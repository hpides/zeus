package de.hpi.des.hdes.engine.shared.join.node;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.shared.join.Bucket;
import de.hpi.des.hdes.engine.shared.join.StreamASource;

public class ASourceNode<IN, KEY> extends UnaryOperationNode<IN, Bucket<KEY, IN>> {

  public ASourceNode(final Node parent, final StreamASource<IN, KEY> operator) {
    super(fromParentID(parent), operator);
  }

  private static String fromParentID(final Node parent) {
    final String nodeId = parent.getNodeId();
    return nodeId.substring(nodeId.length() / 2);
  }

}
