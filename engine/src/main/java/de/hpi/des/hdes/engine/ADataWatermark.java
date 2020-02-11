package de.hpi.des.hdes.engine;

import lombok.Getter;

public class ADataWatermark<T> extends AData<T> {
  @Getter
  private long watermarkTimestamp;

  public ADataWatermark(T value, long eventTime, long watermarkTimestamp) {
    super(value, eventTime, true);
    this.watermarkTimestamp = watermarkTimestamp;
  }

  public static <T> ADataWatermark<T> from(AData<T> event, long watermarkTimestamp) {
    return new ADataWatermark<>(
      event.getValue(),
      event.getEventTime(),
      watermarkTimestamp
    );
  }

  @Override
  public boolean isWatermark() {
    return true;
  }
}
