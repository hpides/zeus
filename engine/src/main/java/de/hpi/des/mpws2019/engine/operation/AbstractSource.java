package de.hpi.des.mpws2019.engine.operation;

public abstract class AbstractSource<V> implements Initializable<V> {

  protected Collector<V> collector = null;

  @Override
  public void init(final Collector<V> collector) {
    this.collector = collector;
  }
}
