package de.hpi.des.hdes.engine.udf;

/**
 * Interface for filter transformations.
 *
 * @param <IN> input type
 */
public interface Filter<IN> {

  boolean filter(IN v);
}
