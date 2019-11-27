package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.Source;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SourceNode<OUT> extends Node {

  private final Source<OUT> source;

  @Override
  public void accept(NodeVisitor visitor) {
    visitor.visit(this);
  }

}
