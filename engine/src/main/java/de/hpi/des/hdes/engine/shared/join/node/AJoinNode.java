package de.hpi.des.hdes.engine.shared.join.node;

import de.hpi.des.hdes.engine.graph.vulcano.BinaryOperationNode;
import de.hpi.des.hdes.engine.shared.join.Bucket;
import de.hpi.des.hdes.engine.shared.join.IntersectedBucket;
import de.hpi.des.hdes.engine.shared.join.StreamAJoin;

public class AJoinNode<IN1, IN2, KEY>
    extends BinaryOperationNode<Bucket<KEY, IN1>, Bucket<KEY, IN2>, IntersectedBucket<IN1, IN2>> {

  public AJoinNode(final String name, final StreamAJoin<IN1, IN2, KEY> operator) {
    super("AJoinNode-" + name, operator);
  }

  public AJoinNode(final String parentIdentifier1, final String parentIdentifier2,
      final StreamAJoin<IN1, IN2, KEY> operator) {
    super(convertToSingleIdentifier(parentIdentifier1, parentIdentifier2), operator);
  }

  private static String convertToSingleIdentifier(final String parentIdentifier1, final String parentIdentifier2) {
    return "AJoin" + parentIdentifier1.substring(0, 10) + parentIdentifier2.substring(0, 10);
  }
}