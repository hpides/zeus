package de.hpi.des.mpws2019.engine.operation;

public abstract class AbstractSource<V> implements Initializable<V> {

  protected Output<V> collector = null;

  @Override
  public void init(final Output<V> collector) {
    this.collector = collector;
  }
}
