package de.hpi.des.hdes.engine.udf;

/**
 * Interface for flatMap transformations.
 *
 * @param <IN> input type
 * @param <OUT> output type
 */
public interface FlatMapper<IN, OUT> {

  /**
   * Maps the input element to an iterable of elements with the OUT type.
   *
   * @param in input element
   * @return iterable of output elements
   */
  Iterable<OUT> flatMap(IN in);
}
