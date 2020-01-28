package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.StreamFilter;
import de.hpi.des.hdes.engine.operation.StreamFlatMap;
import de.hpi.des.hdes.engine.operation.StreamMap;
import de.hpi.des.hdes.engine.udf.Filter;
import de.hpi.des.hdes.engine.udf.FlatMapper;
import de.hpi.des.hdes.engine.udf.Mapper;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;

public class AStream<In> extends AbstractAStream<In> {

  public AStream(final TopologyBuilder builder, final Node node) {
    super(builder, node);
  }

  public <Out> AStream<Out> flatMap(final FlatMapper<? super In, Out> mapper) {
    final UnaryOperationNode<In, Out> child = new UnaryOperationNode<>(new StreamFlatMap<>(mapper));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  public <Out> AStream<Out> map(final Mapper<In, Out> mapper) {
    final UnaryOperationNode<In, Out> child = new UnaryOperationNode<>(new StreamMap<>(mapper));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  public AStream<In> filter(final Filter<? super In> filter) {
    final UnaryOperationNode<In, In> child = new UnaryOperationNode<>(new StreamFilter<>(filter));
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }

  public WindowedAStream<In> window(final WindowAssigner<? extends Window> window) {
    return new WindowedAStream<In>(this.builder, this.node, window);
  }

  public WindowedAStream<In> windowAll() {
    return new WindowedAStream<In>(this.builder, this.node, GlobalWindow.create());
  }

  public TopologyBuilder to(final Sink<? super In> sink) {
    final SinkNode<? super In> sinkNode = new SinkNode<>(sink);
    this.builder.addGraphNode(this.node, sinkNode);
    return this.builder;
  }

}
