package de.hpi.des.hdes.engine.operation;


/**
 * Abstract class for elements of a topology.
 *
 * All such elements use a collector to send their output downstream.
 *
 * @param <OUT> the type of output elements
 */
public abstract class AbstractTopologyElement<OUT> implements Initializable<OUT> {

  protected Collector<OUT> collector = null;

  @Override
  public void init(final Collector<OUT> collector) {
    this.collector = collector;
  }
}
