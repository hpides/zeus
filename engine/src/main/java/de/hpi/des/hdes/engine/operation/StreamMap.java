package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Mapper;
import org.jetbrains.annotations.NotNull;

public class StreamMap<V, VR> extends AbstractTopologyElement<VR> implements OneInputOperator<V, VR> {

  private final Mapper<V, VR> mapper;

  public StreamMap(final Mapper<V, VR> mapper) {
    this.mapper = mapper;
  }

  @Override
  public void process(@NotNull final V value) {
    final VR output = this.mapper.map(value);
    this.collector.collect(output);
  }
}
