package de.hpi.des.mpws2019.engine.stream;

import de.hpi.des.mpws2019.engine.graph.BinaryOperationNode;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.operation.StreamJoin;
import de.hpi.des.mpws2019.engine.udf.Join;
import de.hpi.des.mpws2019.engine.window.Window;
import de.hpi.des.mpws2019.engine.window.assigner.WindowAssigner;
import java.util.function.BiPredicate;

public class WindowedAStream<In> extends AbstractAStream<In> {

  private final WindowAssigner<? extends Window> windowAssigner;

  public WindowedAStream(final TopologyBuilder builder, final Node node,
                         final WindowAssigner<? extends Window> windowAssigner) {
    super(builder, node);
    this.windowAssigner = windowAssigner;
  }

  public <Other, Out> AStream<Out> join(final AStream<Other> other,
                                        final Join<In, Other, Out> join,
                                        final BiPredicate<In, Other> predicate) {
    final BinaryOperationNode<In, Other, Out> child = new BinaryOperationNode<>(
        new StreamJoin<>(join, predicate, this.windowAssigner));
    this.builder.addGraphNode(this.node, child);
    this.builder.addGraphNode(other.getNode(), child);
    return new AStream<>(this.builder, child);
  }
}
