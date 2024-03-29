package de.hpi.des.hdes.engine;

import lombok.Getter;
import lombok.Setter;

/**
 * AData wraps all elements sent through HDES.
 *
 * It contains additional data like the extracted timestamp or watermark.
 *
 * @param <V> the type of the wrapped value
 */
@Getter
public class AData<V> {

  private final V value;
  private final long eventTime;
  private boolean isWatermark;

  private AData(final V value) {
    this.value = value;
    this.eventTime = 0;
  }

  public AData(final V value, final long eventTime, final boolean isWatermark) {
    this.value = value;
    this.eventTime = eventTime;
    this.isWatermark = isWatermark;
  }

  public static <V> AData<V> of(final V value) {
    return new AData<>(value);
  }

  public <W> AData<W> transform(final W value) {
    if (this.isWatermark) {
      final ADataWatermark<W> watermark = (ADataWatermark<W>) this;
      return new ADataWatermark<>(value, this.eventTime, watermark.getWatermarkTimestamp());
    }
    return new AData<>(value, this.eventTime, this.isWatermark);
  }

  public boolean isWatermark() {
    return false;
  }

  @Override
  public String toString() {
    return "AData{" +
        "value=" + this.value +
        ", eventTime=" + this.eventTime +
        '}';
  }
}
