package de.hpi.des.hdes.engine.astream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.graph.vulcano.UnaryOperationNode;
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

/**
 * A windowed AStream.
 *
 * A WindowedAStream should only be created by
 * {@link AStream#window(WindowAssigner)}. The window is defined by a
 * {@link WindowAssigner}.
 *
 * @param <In> the elements of the stream
 */
public class WindowedAStream<In> extends AbstractAStream<In> {

  private static final long ASOURCE_SLICE_FRACTION = 10;

  private final WindowAssigner<? extends Window> windowAssigner;

  public WindowedAStream(final VulcanoTopologyBuilder builder, final Node node,
      final WindowAssigner<? extends Window> windowAssigner) {
    super(builder, node);
    this.windowAssigner = windowAssigner;
  }

  /**
   * Join this stream with another stream.
   *
   * Note: The other stream is implicitly windowed as this stream.
   *
   * @param other              the other stream
   * @param join               selection definition of join
   * @param keyselector1       index selector for the first stream
   * @param keyselector2       index selector for the second stream
   * @param watermarkGenerator watermark generator for the resulting stream
   * @param timestampExtractor timestamp extractor for the resulting stream
   * @param <Other>            type of other streams elements
   * @param <Key>              type of the join key
   * @param <Out>              type of the resulting stream elements
   * @return the resulting AStream
   */
  public <Other, Key, Out> AStream<Out> join(final AStream<Other> other, final Join<In, Other, Out> join,
      final KeySelector<In, Key> keyselector1, final KeySelector<Other, Key> keyselector2,
      final WatermarkGenerator<Out> watermarkGenerator, final TimestampExtractor<Out> timestampExtractor) {
    final BinaryOperationNode<In, Other, Out> child = new BinaryOperationNode<>(new StreamJoin<>(join, keyselector1,
        keyselector2, this.windowAssigner, watermarkGenerator, timestampExtractor));
    this.builder.addGraphNode(this.node, child);
    this.builder.addGraphNode(other.getNode(), child);
    return new AStream<>(this.builder, child);
  }

  /**
   * Groups this stream by a key.
   *
   * @param keyselector the selector to group with
   * @param <KEY>       type of the key
   * @return a stream keyed by the selector and windowed by the current window
   */
  public <KEY> KeyedWindowedAStream<In, KEY> groupBy(final KeySelector<In, KEY> keyselector) {
    return new KeyedWindowedAStream<>(keyselector, this.builder, this.node, this.windowAssigner);
  }

  /**
   * Aggregates this stream.
   *
   * @param aggregator         the aggregator definition
   * @param watermarkGenerator watermark generator for the resulting stream
   * @param timestampExtractor timestamp extractor for the resulting stream
   * @param <Out>              type of the resulting stream elements
   * @param <State>            type of the aggregation's state
   * @return the resulting AStream
   */
  public <Out, State> AStream<Out> aggregate(final Aggregator<In, State, Out> aggregator,
      final WatermarkGenerator<Out> watermarkGenerator, final TimestampExtractor<Out> timestampExtractor) {
    final UnaryOperationNode<In, Out> child = new UnaryOperationNode<>(
        new StreamAggregation<>(aggregator, this.windowAssigner, watermarkGenerator, timestampExtractor));

    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  /**
   * Performs a shared join with another stream.
   *
   * This method uses default watermark generator and extractor. If you apply
   * downstream windowing, this might not work. See
   * {@link #ajoin(AStream, KeySelector, KeySelector, Join, TimestampExtractor, WatermarkGenerator, String)}
   * for custom definitions. Furthermore, this relies on automatic id generation.
   * See the other methods for manual assignment.
   *
   * @param other            the other stream
   * @param inKeySelector    join key selector for the this stream
   * @param otherKeySelector join key selector for the other stream
   * @param join             the join selection definition
   * @param <Other>          type of other streams elements
   * @param <Key>            type of the join key
   * @param <Out>            type of the resulting stream elements
   * @return a stream of joined elements
   */
  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other, final KeySelector<In, Key> inKeySelector,
      final KeySelector<Other, Key> otherKeySelector, final Join<? super In, ? super Other, ? extends Out> join) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.empty(), defaultExtractor(),
        defaultGenerator());
  }

  /**
   * Performs a shared join with another stream.
   *
   * This method uses default watermark generator and extractor. If you apply
   * downstream windowing, this might not work. See
   * {@link #ajoin(AStream, KeySelector, KeySelector, Join, TimestampExtractor, WatermarkGenerator, String)}
   * for custom definitions.
   *
   * This version uses manual id assigment. Joins with the same id are shared.
   *
   * @param other            the other stream
   * @param inKeySelector    join key selector for the this stream
   * @param otherKeySelector join key selector for the other stream
   * @param join             the join selection definition
   * @param name             the id of this ajoin
   * @param <Other>          type of other streams elements
   * @param <Key>            type of the join key
   * @param <Out>            type of the resulting stream elements
   * @return a stream of joined elements
   */
  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other, final KeySelector<In, Key> inKeySelector,
      final KeySelector<Other, Key> otherKeySelector, final Join<? super In, ? super Other, ? extends Out> join,
      final String name) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.of(name), defaultExtractor(),
        defaultGenerator());
  }

  /**
   * Performs a shared join with another stream.
   *
   * This version uses manual id assigment. Joins with the same id are shared.
   *
   * @param other              the other stream
   * @param inKeySelector      join key selector for the this stream
   * @param otherKeySelector   join key selector for the other stream
   * @param join               the join selection definition
   * @param watermarkGenerator watermark generator for the resulting stream
   * @param timestampExtractor timestamp extractor for the resulting stream
   * @param name               the id of this ajoin
   * @param <Other>            type of other streams elements
   * @param <Key>              type of the join key
   * @param <Out>              type of the resulting stream elements
   * @return a stream of joined elements
   */
  public <Other, Out, Key> AStream<Out> ajoin(final AStream<Other> other, final KeySelector<In, Key> inKeySelector,
      final KeySelector<Other, Key> otherKeySelector, final Join<? super In, ? super Other, ? extends Out> join,
      final TimestampExtractor<Out> timestampExtractor, final WatermarkGenerator<Out> watermarkGenerator,
      final String name) {
    return this.createAJoinAStream(other, inKeySelector, otherKeySelector, join, Optional.of(name), timestampExtractor,
        watermarkGenerator);
  }

  /**
   * Internal method to create an shared join.
   *
   * @param other              the other stream
   * @param inKeySelector      join key selector for the this stream
   * @param otherKeySelector   join key selector for the other stream
   * @param join               the join selection definition
   * @param watermarkGenerator watermark generator for the resulting stream
   * @param timestampExtractor timestamp extractor for the resulting stream
   * @param name               the id of this ajoin. If empty, automatic assigment
   *                           is used.
   * @param <Other>            type of other streams elements
   * @param <Key>              type of the join key
   * @param <Out>              type of the resulting stream elements
   * @return a stream of joined elements
   */
  private <Other, Out, Key> AStream<Out> createAJoinAStream(final AStream<Other> other,
      final KeySelector<In, Key> inKeySelector, final KeySelector<Other, Key> otherKeySelector,
      final Join<? super In, ? super Other, ? extends Out> join, final Optional<String> name,
      final TimestampExtractor<Out> timestampExtractor, final WatermarkGenerator<Out> watermarkGenerator) {

    final StreamASource<In, Key> source1 = new StreamASource<>(this.sliceSize(), inKeySelector);
    final StreamASource<Other, Key> source2 = new StreamASource<>(this.sliceSize(), otherKeySelector);
    final StreamAJoin<In, Other, Key> aJoin = new StreamAJoin<>(this.windowAssigner);
    final StreamASink<In, Other, Out> sink = new StreamASink<>(join, timestampExtractor, watermarkGenerator);

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

  /**
   * Internal method to create DAG relationship between AJoin nodes.
   *
   * @param other       the other join stream
   * @param sourceNode1 source node of this stream
   * @param sourceNode2 source node of the other stream
   * @param joinNode    the join node
   * @param sinkNode    the sink node
   * @param <Other>     type of other streams elements
   * @param <Key>       type of the join key
   * @param <Out>       type of the resulting stream elements
   */
  private <Other, Out, Key> void addGraphDependencies(final AStream<Other> other,
      final ASourceNode<In, Key> sourceNode1, final ASourceNode<Other, Key> sourceNode2,
      final AJoinNode<In, Other, Key> joinNode, final ASinkNode<In, Other, Out> sinkNode) {
    this.builder.addGraphNode(this.node, sourceNode1);
    this.builder.addGraphNode(other.getNode(), sourceNode2);
    this.builder.addGraphNode(sourceNode1, joinNode);
    this.builder.addGraphNode(sourceNode2, joinNode);
    this.builder.addGraphNode(joinNode, sinkNode);
  }

  /**
   * @return the slice size to use
   */
  private long sliceSize() {
    return this.windowAssigner.maximumSliceSize() / ASOURCE_SLICE_FRACTION;
  }

  /**
   * @param <Out> type of the outgoing elements
   * @return the default generator
   */
  private static <Out> WatermarkGenerator<Out> defaultGenerator() {
    return new WatermarkGenerator<>(0, 10_000);
  }

  /**
   * @param <Out> type of the outgoing elements
   * @return the default extractor
   */
  private static <Out> TimestampExtractor<Out> defaultExtractor() {
    return elem -> 0;
  }

}
