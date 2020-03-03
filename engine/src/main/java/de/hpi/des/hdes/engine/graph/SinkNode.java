package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.Sink;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SinkNode<IN> extends Node {

  private final Sink<IN> sink;

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }

}
