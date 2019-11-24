package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.Source;
import lombok.Getter;

public class SourceNode<In> extends Node {

  @Getter
  private final Source<In> source;

  public SourceNode(final Source<In> source) {
    this.source = source;
  }

  public Source<In> getSource() {
    return this.source;
  }
}
