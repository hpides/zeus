package de.hpi.des.hdes.engine.shared.join.node;

import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.shared.join.Bucket;
import de.hpi.des.hdes.engine.shared.join.IntersectedBucket;
import de.hpi.des.hdes.engine.shared.join.StreamAJoin;
import de.hpi.des.hdes.engine.shared.join.StreamASink;
import lombok.Getter;

@Getter
public class AJoinNode<IN1, IN2, KEY> extends
    BinaryOperationNode<Bucket<KEY, IN1>, Bucket<KEY, IN2>, IntersectedBucket<IN1, IN2>> {

  private final StreamASink<IN1, IN2, ?> sink;

  public AJoinNode(final String parentIdentifier1, final String parentIdentifier2,
      final StreamAJoin<IN1, IN2, KEY> operator, final StreamASink<IN1, IN2, ?> sink) {
    super(convertToSingleIdentifier(parentIdentifier1, parentIdentifier2), operator);
    this.sink = sink;
  }

  public StreamASink<IN1, IN2, ?> getSink() {
    return this.sink;
  }

  private static String convertToSingleIdentifier(final String parentIdentifier1,
      final String parentIdentifier2) {
    return "AJoin" + parentIdentifier1.substring(0, 10) + parentIdentifier2.substring(0, 10);
  }

}