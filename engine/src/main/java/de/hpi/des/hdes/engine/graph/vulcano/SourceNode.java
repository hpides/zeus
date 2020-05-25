package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.Source;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents sources in the logical plan.
 *
 * @param <OUT> type of outgoing elements
 */
@Getter
public class SourceNode<OUT> extends Node {

  private final Source<OUT> source;

  public SourceNode(final Source<OUT> source) {
    super("SourceNode-" + source.getIdentifier());
    this.source = source;
  }

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }
}
