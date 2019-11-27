package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.Sink;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SinkNode<IN> extends Node {

  private final Sink sink;

  @Override
  public void accept(NodeVisitor visitor) {
    visitor.visit(this);
  }

}
