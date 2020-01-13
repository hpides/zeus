package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.ajoin.Bucket;
import de.hpi.des.hdes.engine.ajoin.IntersectedBucket;
import de.hpi.des.hdes.engine.ajoin.StreamAJoin;
import de.hpi.des.hdes.engine.ajoin.StreamASink;
import de.hpi.des.hdes.engine.ajoin.StreamASource;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.StreamAggregation;
import de.hpi.des.hdes.engine.operation.StreamJoin;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
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

  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector,
      final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join) {
    // todo use triggerInterval based on window assigner
    final StreamASource<In, Key> source1 = new StreamASource<>(50, this.windowAssigner,
        inKeySelector);
    final StreamASource<Other, Key> source2 = new StreamASource<>(50, this.windowAssigner,
        otherKeySelector);
    final StreamAJoin<In, Other, Key> aJoin = new StreamAJoin<>();
    final StreamASink<In, Other, Out> sink = new StreamASink<>(join);
    final UnaryOperationNode<In, Bucket<Key, In>> sourceNode1 = new UnaryOperationNode<>(source1);
    final UnaryOperationNode<Other, Bucket<Key, Other>> sourceNode2 = new UnaryOperationNode<>(source2);
    final BinaryOperationNode<Bucket<Key, In>, Bucket<Key, Other>, IntersectedBucket<In, Other>> joinNode =
        new BinaryOperationNode<>(aJoin);
    final UnaryOperationNode<IntersectedBucket<In, Other>, Out> sinkNode = new UnaryOperationNode<>(sink);

    this.builder.addGraphNode(this.node, sourceNode1);
    this.builder.addGraphNode(other.getNode(), sourceNode2);
    this.builder.addGraphNode(sourceNode1, joinNode);
    this.builder.addGraphNode(sourceNode2, joinNode);
    this.builder.addGraphNode(joinNode, sinkNode);

    return new AStream<>(this.builder, sinkNode);
  }

  public <OUT, TYPE> AStream<OUT> aggregate(final Aggregator<In, TYPE, OUT> aggregator) {
    final UnaryOperationNode<In, OUT> child = new UnaryOperationNode<>(
        new StreamAggregation<>(aggregator, this.windowAssigner));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }
}
