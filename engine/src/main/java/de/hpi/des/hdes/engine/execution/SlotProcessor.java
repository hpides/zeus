package de.hpi.des.hdes.engine.execution;

/**
 * Common interface for downstream processor in a slot.
 *
 * @param <IN> type of incoming elements
 */
public interface SlotProcessor<IN> {

  /**
   * Pushes element into this processor
   *
   * @param event event to send downstream
   */
  void sendDownstream(IN event);
}
