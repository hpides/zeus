package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Mapper;

public class StreamMap<V, VR> extends AbstractInitializable<VR> implements OneInputOperator<V, VR> {

  private final Mapper<V, VR> mapper;

  public StreamMap(final Mapper<V, VR> mapper) {
    this.mapper = mapper;
  }

  @Override
  public void process(final V value) {
    final VR output = this.mapper.map(value);
    this.collector.collect(output);
  }
}
