package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.Source;
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
