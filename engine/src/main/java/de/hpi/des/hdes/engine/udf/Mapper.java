package de.hpi.des.hdes.engine.udf;

/**
 * Interface for map transformations.
 *
 * @param <IN> input type
 * @param <OUT> output type
 */
public interface Mapper<IN, OUT> {

  /**
   * Maps the input element to an element of the output type.
   *
   * @param in input element
   * @return transformed output element
   */
  OUT map(IN in);
}
