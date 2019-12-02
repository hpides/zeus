package de.hpi.des.hdes.engine.udf;

public interface Filter<IN> {

  boolean filter(IN v);
}
