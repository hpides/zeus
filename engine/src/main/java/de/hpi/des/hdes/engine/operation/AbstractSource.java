package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.AEventProfiling;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * Base class that simplifies the definition of custom sources.
 * @param <OUT> produced event type
 */
@RequiredArgsConstructor
public abstract class AbstractSource<OUT> extends AbstractTopologyElement<OUT> implements Source<OUT> {
  private final TimestampExtractor<OUT> timestampExtractor;
  private final WatermarkGenerator<OUT> watermarkGenerator;
  @Setter
  private boolean profilingEvents = false;

  /**
   * Has to be implemented by each AbstractSource to read events from.
   *
   * @return event read out of the source
   */
  abstract public OUT readEvent();

  /**
   * Passes the event read from the source downstream.
   * Additionaly it is wrapped inside AData to store the metadata
   * and a watermark timestamp is attached to some events.
   *
   * @param event the event to be passed downstream
   */
  public void collectEvent(OUT event) {
    final AData<OUT> wrapped = new AData<>(event, timestampExtractor.apply(event), false);
    final AData<OUT> watermarked = watermarkGenerator.apply(wrapped);
    this.collector.collect(watermarked);
  }

  public void collectProfilingEvent(OUT event) {
    final AEventProfiling<OUT> wrapped = new AEventProfiling<>(event, timestampExtractor.apply(event), false, System.currentTimeMillis());
    final AData<OUT> watermarked = watermarkGenerator.apply(wrapped);
    this.collector.collect(watermarked);
  }

  /**
   * Reads an event from the source.
   */
  public void read() {
    OUT event = readEvent();
    if (event != null) {
      if (this.profilingEvents) {
        collectProfilingEvent(event);
      } else {
        collectEvent(event);
      }
    }
  }

}
