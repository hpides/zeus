package de.hpi.des.hdes.engine.udf;

public interface KeySelector<IN, KEY> {
  KEY selectKey(IN in);
}
