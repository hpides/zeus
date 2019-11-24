package de.hpi.des.mpws2019.engine.operation;


public abstract class AbstractOperation<OUT> implements Initializable<OUT> {

  protected Output<OUT> collector = null;

  @Override
  public void init(final Output<OUT> collector) {
    this.collector = collector;
  }
}
