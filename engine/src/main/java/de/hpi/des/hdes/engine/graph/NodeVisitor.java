package de.hpi.des.hdes.engine.graph;

public interface NodeVisitor {

  <OUT> void visit(SourceNode<OUT> sourceNode);

  <IN> void visit(SinkNode<IN> sinkNode);

  <IN, OUT> void visit(UnaryOperationNode<IN, OUT> unaryOperationNode);

  <IN1, IN2, OUT> void visit(BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode);

}
