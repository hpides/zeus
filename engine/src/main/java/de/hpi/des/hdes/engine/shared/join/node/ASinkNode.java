package de.hpi.des.hdes.engine.shared.join.node;

import de.hpi.des.hdes.engine.graph.vulcano.UnaryOperationNode;
import de.hpi.des.hdes.engine.shared.join.IntersectedBucket;
import de.hpi.des.hdes.engine.shared.join.StreamASink;

public class ASinkNode<IN1, IN2, OUT> extends UnaryOperationNode<IntersectedBucket<IN1, IN2>, OUT> {

  public ASinkNode(final String name, final StreamASink<IN1, IN2, OUT> operator) {
    super("ASink-" + name + operator.toString(), operator);
  }

  public ASinkNode(final StreamASink<IN1, IN2, OUT> operator) {
    super("ASink-" + operator.toString(), operator);
  }

}
