package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Mapper;

public class StreamMap<V, VR> extends AbstractTopologyElement<VR> implements OneInputOperator<V, VR> {

  private final Mapper<V, VR> mapper;

  public StreamMap(final Mapper<V, VR> mapper) {
    this.mapper = mapper;
  }

  @Override
  public void process(final AData<V> aData) {
    final VR output = this.mapper.map(aData.getValue());
    var newAData = aData.transform(output);
    this.collector.collect(newAData);
  }
}
