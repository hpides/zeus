package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.graph.vulcano.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.vulcano.SinkNode;
import de.hpi.des.hdes.engine.graph.vulcano.SourceNode;
import de.hpi.des.hdes.engine.graph.vulcano.UnaryOperationNode;

/**
 * A NodeVisitor can be implemented to iterate through a collection of node with
 * type safety.
 *
 */
public interface NodeVisitor {

  <OUT> void visit(SourceNode<OUT> sourceNode);

  <IN> void visit(SinkNode<IN> sinkNode);

  <IN, OUT> void visit(UnaryOperationNode<IN, OUT> unaryOperationNode);

  <IN1, IN2, OUT> void visit(BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode);
}
