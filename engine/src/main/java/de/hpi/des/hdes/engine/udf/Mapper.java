package de.hpi.des.hdes.engine.udf;

public interface Mapper<IN, OUT> {

  OUT map(IN in);
}
