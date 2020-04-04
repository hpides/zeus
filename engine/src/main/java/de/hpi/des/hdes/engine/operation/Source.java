package de.hpi.des.hdes.engine.operation;

/**
 * A source reads elements from an external entity and ingests them into HDES.
 *
 * @param <OUT> type of outgoing elements
 */
public interface Source<OUT> extends Initializable<OUT> {

  String getIdentifier();

  void read();
}
