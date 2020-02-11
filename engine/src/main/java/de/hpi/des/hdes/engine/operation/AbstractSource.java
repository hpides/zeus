package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractSource<OUT> extends AbstractTopologyElement<OUT> implements Source<OUT> {
  private final TimestampExtractor<OUT> timestampExtractor;
  private final WatermarkGenerator<OUT> watermarkGenerator;

  abstract public OUT readEvent();

  public void collectEvent(OUT event) {
    final AData<OUT> wrapped = new AData<>(event, timestampExtractor.apply(event), false);
    final AData<OUT> watermarked = watermarkGenerator.apply(wrapped);
    this.collector.collect(watermarked);
  }

  public void read() {
    OUT event = readEvent();
    if (event != null) {
      collectEvent(event);
    }
  }

}
