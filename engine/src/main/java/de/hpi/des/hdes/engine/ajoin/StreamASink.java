package de.hpi.des.hdes.engine.ajoin;

import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.udf.Join;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamASink<IN1, IN2, OUT> extends AbstractTopologyElement<OUT> implements
    OneInputOperator<IntersectedBucket<IN1, IN2>, OUT> {

  // converts buckets into stream tuples
  // output (in our case send downstream?)
  private final Join<? super IN1, ? super IN2, ? extends OUT> join;

  public StreamASink(final Join<? super IN1, ? super IN2, ? extends OUT> join) {
    this.join = join;
  }

  @Override
  public void process(final IntersectedBucket<IN1, IN2> inBucket) {
    for (final IN1 in1 : inBucket.getV1()) {
      for (final IN2 in2 : inBucket.getV2()) {
        this.collector.collect(this.join.join(in1, in2));
      }
    }
  }
}
