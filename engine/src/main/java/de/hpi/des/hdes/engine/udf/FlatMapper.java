package de.hpi.des.hdes.engine.udf;

public interface FlatMapper<IN, OUT> {

  Iterable<OUT> flatMap(IN in);
}
