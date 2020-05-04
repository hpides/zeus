package de.hpi.des.hdes.engine;

import lombok.Getter;
import lombok.Setter;

/**
 * AEventProfiling extends AData, which wraps all elements sent through HDES.
 *
 * It extends the AData object by information only necessary for profiling.
 *
 * @param <V> the type of the wrapped value
 */

 @Getter
 @Setter
 public class AEventProfiling<V> extends AData<V> {

    private long proccesingTime;
    private long ejectionTime;

    public AEventProfiling(V value, long eventTime, boolean isWatermark) {
        super(value, eventTime, isWatermark);
        proccesingTime = -1;
        ejectionTime = -1; 
    }

    public AEventProfiling(V value, long eventTime, boolean isWatermark, long proccesingTime) {
        super(value, eventTime, isWatermark);
        this.proccesingTime = proccesingTime;
        ejectionTime = -1;
    }

    public AEventProfiling(V value, long eventTime, boolean isWatermark, long proccesingTime, long ejectionTime) {
        super(value, eventTime, isWatermark);
        this.proccesingTime = proccesingTime;
        this.ejectionTime = ejectionTime;
    }

    @Override
    public <W> AData<W> transform(final W value) {
        var superResult = super.transform(value);
        if (super.isWatermark()) {
          return superResult;
        }
        return new AEventProfiling<>(value, super.getEventTime(), super.isWatermark(), this.getProccesingTime(), this.getEjectionTime());
      }
     
 }