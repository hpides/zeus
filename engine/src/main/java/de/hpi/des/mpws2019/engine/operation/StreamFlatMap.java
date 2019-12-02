package de.hpi.des.mpws2019.engine.operation;

import de.hpi.des.mpws2019.engine.udf.FlatMapper;

public class StreamFlatMap<IN, OUT> extends AbstractInitializable<OUT>
    implements OneInputOperator<IN, OUT> {

  private final FlatMapper<? super IN, OUT> flatMapper;

  public StreamFlatMap(final FlatMapper<? super IN, OUT> flatMapper) {
    this.flatMapper = flatMapper;
  }

  @Override
  public void process(final IN in) {
    final Iterable<OUT> result = this.flatMapper.flatMap(in);
    for (final OUT out : result) {
      this.collector.collect(out);
    }
  }
}
