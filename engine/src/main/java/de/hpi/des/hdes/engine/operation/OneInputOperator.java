package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.SlotProcessor;

/**
 * An operator that has on input stream
 *
 * @param <IN>  the input type of the stream elements
 * @param <OUT> the key type of the join
 */
public interface OneInputOperator<IN, OUT> extends Operator<OUT>, SlotProcessor<AData<IN>> {

  /**
   * Processes incoming elements
   *
   * @param in incoming element
   */
  void process(AData<IN> in);

  @Override
  default void sendDownstream(final AData<IN> event) {
    this.process(event);
  }
}
