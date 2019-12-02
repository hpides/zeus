package de.hpi.des.mpws2019.engine.udf;

public interface Filter<IN> {

  boolean filter(IN v);
}
