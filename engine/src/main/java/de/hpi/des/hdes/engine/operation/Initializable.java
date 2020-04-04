package de.hpi.des.hdes.engine.operation;

/**
 * An initializable can be used to set the downstream collector.
 *
 * @param <OUT> the type of outgoing elements
 */
public interface Initializable<OUT> {

  void init(Collector<OUT> collector);
}
