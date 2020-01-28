package de.hpi.des.hdes.engine.udf;

@FunctionalInterface
public interface Join<IN1, IN2, OUT> {

  OUT join(IN1 first, IN2 second);
}