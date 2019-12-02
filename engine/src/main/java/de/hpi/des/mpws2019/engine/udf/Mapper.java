package de.hpi.des.mpws2019.engine.udf;

public interface Mapper<IN, OUT> {

  OUT map(IN in);
}
