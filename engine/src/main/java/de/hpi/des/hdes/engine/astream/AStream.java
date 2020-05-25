package de.hpi.des.hdes.engine.astream;

import java.io.IOException;

import org.jooq.lambda.tuple.Tuple3;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.SinkNode;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.graph.vulcano.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.StreamFilter;
import de.hpi.des.hdes.engine.operation.StreamFlatMap;
import de.hpi.des.hdes.engine.operation.StreamMap;
import de.hpi.des.hdes.engine.udf.Filter;
import de.hpi.des.hdes.engine.operation.StreamFlatProfilingMap;
import de.hpi.des.hdes.engine.udf.FlatMapper;
import de.hpi.des.hdes.engine.udf.Mapper;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;

/**
 * Plain AStream without any additional state.
 *
 * @param <In> type of the elements
 */
public class AStream<In> extends AbstractAStream<In> {

  public AStream(final VulcanoTopologyBuilder builder, final Node node) {
    super(builder, node);
  }

  /**
   * Flat maps the elements of this stream.
   *
   * @param mapper the mapper
   * @param <Out>  type of the outgoing elements
   * @return the resulting stream with flat mapped elements
   */
  public <Out> AStream<Out> flatMap(final FlatMapper<? super In, Out> mapper) {
    final UnaryOperationNode<In, Out> child = new UnaryOperationNode<>(new StreamFlatMap<>(mapper));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  /**
   * Maps the elements of this stream.
   *
   * @param mapper the mapper
   * @param <Out>  type of the outgoing elements
   * @return the resulting stream with flat mapped elements
   */
  public <Out> AStream<Out> map(final Mapper<In, Out> mapper) {
    final UnaryOperationNode<In, Out> child = new UnaryOperationNode<>(new StreamMap<>(mapper));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  public AStream<Tuple3<Long, Long, Long>> flatProfiling() {
    final UnaryOperationNode<In, Tuple3<Long, Long, Long>> child = new UnaryOperationNode<>(
        new StreamFlatProfilingMap<>());
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  /**
   * Filters the elements of this stream.
   *
   * Only elements fulfilling the filter predicate remain in the stream.
   *
   * @param filter the predicate
   * @return the filtered stream
   */
  public AStream<In> filter(final Filter<? super In> filter) {
    final UnaryOperationNode<In, In> child = new UnaryOperationNode<>(new StreamFilter<>(filter));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  /**
   * Windows this stream.
   *
   * @param window the window assigner for the window
   * @return a streamed windowed by the window defined in the assigner
   */
  public WindowedAStream<In> window(final WindowAssigner<? extends Window> window) {
    return new WindowedAStream<In>(this.builder, this.node, window);
  }

  /**
   * Writes the elements of this stream into a sink.
   *
   * @param sink the sink to write elements to
   * @return the final builder
   */
  public VulcanoTopologyBuilder to(final Sink<? super In> sink) {
    final SinkNode<? super In> sinkNode = new SinkNode<>(sink);
    this.builder.addGraphNode(this.node, sinkNode);
    return this.builder;
  }

}
