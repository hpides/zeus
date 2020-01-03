package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.FlatMapper;
import org.jetbrains.annotations.NotNull;

public class StreamFlatMap<IN, OUT> extends AbstractInitializable<OUT>
    implements OneInputOperator<IN, OUT> {

  private final FlatMapper<? super IN, OUT> flatMapper;

  public StreamFlatMap(final FlatMapper<? super IN, OUT> flatMapper) {
    this.flatMapper = flatMapper;
  }

  @Override
  public void process(@NotNull final IN in) {
    final Iterable<OUT> result = this.flatMapper.flatMap(in);
    for (final OUT out : result) {
      this.collector.collect(out);
    }
  }
}
