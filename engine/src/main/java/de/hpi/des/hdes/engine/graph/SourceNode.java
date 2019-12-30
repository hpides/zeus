package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.Source;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
public class SourceNode<OUT> extends Node {

  private final Source<OUT> source;

  public SourceNode(Source<OUT> source) {
    this.source = source;
    this.setNodeId(UUID.nameUUIDFromBytes(Integer.toString(source.hashCode()).getBytes()));
  }

  @Override
  public void accept(NodeVisitor visitor) {
    visitor.visit(this);
  }
}
