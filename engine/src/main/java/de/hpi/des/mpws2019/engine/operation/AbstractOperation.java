package de.hpi.des.mpws2019.engine.operation;


public abstract class AbstractOperation<OUT> implements Initializable<OUT> {

  protected Collector<OUT> collector = null;

  @Override
  public void init(final Collector<OUT> collector) {
    this.collector = collector;
  }
}
