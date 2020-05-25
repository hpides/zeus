package de.hpi.des.hdes.engine.graph.vulcano;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.operation.Sink;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents sinks in the logical plan
 *
 * @param <IN> type of the incoming elements
 */
@RequiredArgsConstructor
@Getter
public class SinkNode<IN> extends Node {

  private final Sink<IN> sink;

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }

}
