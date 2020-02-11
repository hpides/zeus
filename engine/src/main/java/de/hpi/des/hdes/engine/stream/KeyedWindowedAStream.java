package de.hpi.des.hdes.engine.stream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.StreamKeyedAggregation;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;

import java.util.function.Function;

public class KeyedWindowedAStream<IN, KEY> extends AbstractAStream<IN> {

  private final WindowAssigner<? extends Window> windowAssigner;
  private final KeySelector<IN, KEY> keyselector;

  public KeyedWindowedAStream(KeySelector<IN, KEY> keyselector, final TopologyBuilder builder, final Node node,
                              final WindowAssigner<? extends Window> windowAssigner) {
    super(builder, node);
    this.windowAssigner = windowAssigner;
    this.keyselector = keyselector;
  }

  public <OUT, STATE> AStream<OUT> aggregate(
    final Aggregator<IN, STATE, OUT> aggregator,
    final WatermarkGenerator<OUT> watermarkGenerator,
    final TimestampExtractor<OUT> timestampExtractor) {
    StreamKeyedAggregation<IN, KEY, STATE, OUT> streamAgg = new StreamKeyedAggregation(
      this.keyselector,
      aggregator,
      this.windowAssigner,
      watermarkGenerator,
      timestampExtractor);

    final UnaryOperationNode<IN, OUT> child = new UnaryOperationNode<>(streamAgg);
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }
}
