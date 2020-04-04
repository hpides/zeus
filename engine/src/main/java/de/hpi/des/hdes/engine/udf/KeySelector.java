package de.hpi.des.hdes.engine.udf;

/**
 * Used to retrieve keys from generic objects.
 *
 * @param <IN> type of the input object
 * @param <KEY> type of the key
 */
public interface KeySelector<IN, KEY> {
  /**
   * Selects the key from the input object.
   *
   * @param in input object
   * @return extracted key
   */
  KEY selectKey(IN in);
}
