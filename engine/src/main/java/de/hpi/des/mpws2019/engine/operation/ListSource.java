package de.hpi.des.mpws2019.engine.operation;

import java.util.List;

public class ListSource<OUT> extends AbstractSource<OUT> implements Source<OUT> {

  private final List<OUT> list;
  private int i = 0;

  public ListSource(final List<OUT> list) {
    this.list = list;
  }

  @Override
  public void read() {
    if (this.i < this.list.size()) {
      this.collector.collect(this.list.get(this.i++));
    }
  }
}
