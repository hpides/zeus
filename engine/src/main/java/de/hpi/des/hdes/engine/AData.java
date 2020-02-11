package de.hpi.des.hdes.engine;

import lombok.Getter;
import lombok.Setter;

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

  public static <V> AData<V> of(V value) {
    return new AData<>(value);
  }

  public <W> AData<W> transform(W value) {
    if (isWatermark) {
      ADataWatermark watermark = (ADataWatermark) this;
      return new ADataWatermark(value, this.eventTime, watermark.getWatermarkTimestamp());
    }
    return new AData<>(value, this.eventTime, isWatermark);
  }

  public boolean isWatermark() {
    return false;
  }

  @Override
  public String toString() {
    return "AData{" +
        "value=" + value +
        ", eventTime=" + eventTime +
        '}';
  }
}
