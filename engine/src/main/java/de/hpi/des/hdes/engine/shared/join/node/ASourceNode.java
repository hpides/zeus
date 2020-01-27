package de.hpi.des.hdes.engine.shared.join.node;

import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.shared.join.Bucket;
import de.hpi.des.hdes.engine.shared.join.StreamASource;

public class ASourceNode<IN, KEY> extends UnaryOperationNode<IN, Bucket<KEY, IN>> {
  public ASourceNode(final String identifier, final StreamASource<IN, KEY> operator) {
    super(convertIdentifier(identifier), operator);
  }
  private static String convertIdentifier(final String parentIdentifier) {
    return "A-" + parentIdentifier;
  }
}
