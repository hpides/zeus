package de.hpi.des.mpws2019.engine.stream;

import de.hpi.des.mpws2019.engine.graph.BinaryOperationNode;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.SinkNode;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.graph.UnaryOperationNode;
import de.hpi.des.mpws2019.engine.operation.Sink;
import de.hpi.des.mpws2019.engine.operation.StreamFilter;
import de.hpi.des.mpws2019.engine.operation.StreamFlatMap;
import de.hpi.des.mpws2019.engine.operation.StreamMap;
import de.hpi.des.mpws2019.engine.udf.Filter;
import de.hpi.des.mpws2019.engine.udf.FlatMapper;
import de.hpi.des.mpws2019.engine.udf.Join;
import de.hpi.des.mpws2019.engine.udf.Mapper;
import de.hpi.des.mpws2019.engine.window.Window;
import de.hpi.des.mpws2019.engine.window.assigner.GlobalWindow;
import de.hpi.des.mpws2019.engine.window.assigner.WindowAssigner;

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
    return new WindowedAStream<>(this.builder, this.node, window);
  }

  public WindowedAStream<In> windowAll() {
    return new WindowedAStream<>(this.builder, this.node, GlobalWindow.create());
  }

  public void to(final Sink<In> sink) {
    final SinkNode<In> sinkNode = new SinkNode<>(sink);
    this.builder.addGraphNode(this.node, sinkNode);
  }

}
