package de.hpi.des.hdes.engine.graph;

public interface NodeVisitor {
  void visit(SourceNode sourceNode);
  void visit(SinkNode sinkNode);
  void visit(UnaryOperationNode unaryOperationNode);
  void visit(BinaryOperationNode binaryOperationNode);
}
