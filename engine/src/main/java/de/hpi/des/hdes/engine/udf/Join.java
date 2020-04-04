package de.hpi.des.hdes.engine.udf;

/**
 * Interface for join transformations.
 * This function is applied for joinable pairs of events.
 *
 * @param <IN1> input type first stream
 * @param <IN2> input type second stream
 * @param <OUT> output type
 */
@FunctionalInterface
public interface Join<IN1, IN2, OUT> {

  /**
   * Transforms
   * @param first element of first stream
   * @param second element of second stream
   * @return transformed joined element
   */
  OUT join(IN1 first, IN2 second);
}
