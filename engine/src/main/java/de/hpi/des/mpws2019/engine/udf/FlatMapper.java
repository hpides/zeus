package de.hpi.des.mpws2019.engine.udf;

public interface FlatMapper<IN, OUT> {

  Iterable<OUT> flatMap(IN in);
}
