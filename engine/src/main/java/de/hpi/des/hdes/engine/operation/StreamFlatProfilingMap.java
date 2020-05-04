package de.hpi.des.hdes.engine.operation;

import org.jooq.lambda.tuple.Tuple3;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.AEventProfiling;
import de.hpi.des.hdes.engine.udf.Mapper;

public class StreamFlatProfilingMap<V, VR> extends AbstractTopologyElement<VR> implements OneInputOperator<V, VR> {


  @Override
  public void process(final AData<V> aData) {
    if(aData.isWatermark())
        return;
    final var aEvent = (AEventProfiling<V>) aData;
    final VR output = (VR) new Tuple3<Long,Long,Long>(aEvent.getEventTime(), aEvent.getProccesingTime(), System.currentTimeMillis());
    final var newAData = aData.transform(output);
    this.collector.collect(newAData);
  }
}
