package de.hpi.des.hdes.engine.astream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.graph.vulcano.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.StreamKeyedAggregation;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;

/**
 * A stream that is windowed and keyed.
 *
 * The stream contains a {@link WindowAssigner} determining the window. A
 * {@link KeySelector} is responsible for the keying.
 *
 * @param <IN>  type of the stream's elements
 * @param <KEY> type of the key
 */
public class KeyedWindowedAStream<IN, KEY> extends AbstractAStream<IN> {

  private final WindowAssigner<? extends Window> windowAssigner;
  private final KeySelector<IN, KEY> keyselector;

  public KeyedWindowedAStream(KeySelector<IN, KEY> keyselector, final VulcanoTopologyBuilder builder, final Node node,
      final WindowAssigner<? extends Window> windowAssigner) {
    super(builder, node);
    this.windowAssigner = windowAssigner;
    this.keyselector = keyselector;
  }

  /**
   * Aggregates this stream by window and key.
   *
   * @param aggregator         the definition of the aggregation
   * @param watermarkGenerator watermark generator for the resulting stream
   * @param timestampExtractor timestamp extractor for the resulting stream
   * @param <OUT>              type of the resulting stream
   * @param <STATE>            type of the aggregation's state
   * @return a stream of aggregated elements
   */
  public <OUT, STATE> AStream<OUT> aggregate(final Aggregator<IN, STATE, OUT> aggregator,
      final WatermarkGenerator<OUT> watermarkGenerator, final TimestampExtractor<OUT> timestampExtractor) {
    StreamKeyedAggregation<IN, KEY, STATE, OUT> streamAgg = new StreamKeyedAggregation(this.keyselector, aggregator,
        this.windowAssigner, watermarkGenerator, timestampExtractor);

    final UnaryOperationNode<IN, OUT> child = new UnaryOperationNode<>(streamAgg);
    this.builder.addGraphNode(this.node, child);
    return new AStream<>(this.builder, child);
  }
}
