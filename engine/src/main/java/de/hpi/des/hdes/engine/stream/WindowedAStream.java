package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.StreamAggregation;
import de.hpi.des.hdes.engine.operation.StreamJoin;
import de.hpi.des.hdes.engine.shared.join.StreamAJoin;
import de.hpi.des.hdes.engine.shared.join.StreamASink;
import de.hpi.des.hdes.engine.shared.join.StreamASource;
import de.hpi.des.hdes.engine.shared.join.node.AJoinNode;
import de.hpi.des.hdes.engine.shared.join.node.ASinkNode;
import de.hpi.des.hdes.engine.shared.join.node.ASourceNode;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.Optional;

public class WindowedAStream<In> extends AbstractAStream<In> {

  private static final long ASOURCE_SLICE_FRACTION = 10;

  private final WindowAssigner<? extends Window> windowAssigner;

  public WindowedAStream(final TopologyBuilder builder, final Node node,
      final WindowAssigner<? extends Window> windowAssigner) {
    super(builder, node);
    this.windowAssigner = windowAssigner;
  }

  public <Other, Key, Out> AStream<Out> join(final AStream<Other> other,
      final Join<In, Other, Out> join,
      final KeySelector<In, Key> keyselector1,
      final KeySelector<Other, Key> keyselector2,
      final WatermarkGenerator<Out> watermarkGenerator,
      final TimestampExtractor<Out> timestampExtractor
  ) {
    final BinaryOperationNode<In, Other, Out> child = new BinaryOperationNode<>(
        new StreamJoin<>(join, keyselector1, keyselector2, this.windowAssigner, watermarkGenerator,
            timestampExtractor));
    this.builder.addGraphNode(this.node, child);
    this.builder.addGraphNode(other.getNode(), child);
    return new AStream<>(this.builder, child);
  }


  public <KEY> KeyedWindowedAStream<In, KEY> groupBy(final KeySelector<In, KEY> keyselector) {
    return new KeyedWindowedAStream<>(keyselector, this.builder, this.node, this.windowAssigner);
  }

  public <OUT, TYPE> AStream<OUT> aggregate(
      final Aggregator<In, TYPE, OUT> aggregator,
      final WatermarkGenerator<OUT> watermarkGenerator,
      final TimestampExtractor<OUT> timestampExtractor) {
    final UnaryOperationNode<In, OUT> child = new UnaryOperationNode<>(
        new StreamAggregation<>(aggregator, this.windowAssigner, watermarkGenerator,
            timestampExtractor));

    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector, final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.empty(),
        defaultExtractor(), defaultGenerator());
  }

  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector, final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join, final String name) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.of(name),
        defaultExtractor(), defaultGenerator());
  }

  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector, final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join,
      final TimestampExtractor<Out> timestampExtractor,
      final WatermarkGenerator<Out> watermarkGenerator, final String name) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.of(name),
        timestampExtractor, watermarkGenerator);
  }

  private <Other, Out, Key> AStream<Out> createAJoinAStream(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector, final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join, final Optional<String> name,
      final TimestampExtractor<Out> timestampExtractor,
      final WatermarkGenerator<Out> generator) {

    final StreamASource<In, Key> source1 = new StreamASource<>(this.sliceSize(), inKeySelector);
    final StreamASource<Other, Key> source2 = new StreamASource<>(this.sliceSize(),
        otherKeySelector);
    final StreamAJoin<In, Other, Key> aJoin = new StreamAJoin<>(this.windowAssigner);
    final StreamASink<In, Other, Out> sink = new StreamASink<>(join, timestampExtractor, generator);

    final ASourceNode<In, Key> sourceNode1;
    final ASourceNode<Other, Key> sourceNode2;
    final AJoinNode<In, Other, Key> joinNode;
    final ASinkNode<In, Other, Out> sinkNode;
    if (name.isPresent()) {
      sourceNode1 = new ASourceNode<>(name.get(), 1, source1);
      sourceNode2 = new ASourceNode<>(name.get(), 2, source2);
      joinNode = new AJoinNode<>(name.get(), aJoin);
      sinkNode = new ASinkNode<>(name.get(), sink);
    } else {
      sourceNode1 = new ASourceNode<>(this.node, source1);
      sourceNode2 = new ASourceNode<>(other.getNode(), source2);
      joinNode = new AJoinNode<>(sourceNode1.getNodeId(), sourceNode2.getNodeId(), aJoin);
      sinkNode = new ASinkNode<>(sink);
    }

    this.addGraphDependencies(other, sourceNode1, sourceNode2, joinNode, sinkNode);

    return new AStream<>(this.builder, sinkNode);
  }

  private <Other, Out, Key> void addGraphDependencies(final AStream<Other> other,
      final ASourceNode<In, Key> sourceNode1, final ASourceNode<Other, Key> sourceNode2,
      final AJoinNode<In, Other, Key> joinNode, final ASinkNode<In, Other, Out> sinkNode) {
    this.builder.addGraphNode(this.node, sourceNode1);
    this.builder.addGraphNode(other.getNode(), sourceNode2);
    this.builder.addGraphNode(sourceNode1, joinNode);
    this.builder.addGraphNode(sourceNode2, joinNode);
    this.builder.addGraphNode(joinNode, sinkNode);
  }

  private long sliceSize() {
    return this.windowAssigner.maximumSliceSize() / ASOURCE_SLICE_FRACTION;
  }

  private static <Out> WatermarkGenerator<Out> defaultGenerator() {
    return new WatermarkGenerator<>(0, 10_000);
  }

  private static <Out> TimestampExtractor<Out> defaultExtractor() {
    return elem -> 0;
  }

}
