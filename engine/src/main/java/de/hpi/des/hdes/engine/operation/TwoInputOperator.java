package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.Closeable;

/**
 * TwoInputOperator processes two streams.
 *
 * @param <IN1> the input type of the left stream
 * @param <IN2> the input type of the right stream
 * @param <OUT> the output type of this operator
 */
public interface TwoInputOperator<IN1, IN2, OUT> extends Operator<OUT>, Closeable {

  /**
   * Processes the first stream.
   *
   * @param in the input element
   */
  void processStream1(AData<IN1> in);

  /**
   * Processes the right stream.
   *
   * @param in the right element
   */
  void processStream2(AData<IN2> in);
}
