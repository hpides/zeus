package de.hpi.des.hdes.engine.udf;

public interface SharedAggregator<IN, STATE, OUT> extends Aggregator<IN, STATE, OUT> {

  STATE combine(STATE state1, STATE state2);

}
