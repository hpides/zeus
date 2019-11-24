package de.hpi.des.mpws2019.engine.operation;

import java.util.List;
import lombok.Getter;

public class ListSink<IN> extends AbstractOperation<Void> implements Sink<IN> {

  @Getter
  private final List<IN> list;

  public ListSink(final List<IN> list) {
    this.list = list;
  }

  @Override
  public void process(final IN in) {
    this.list.add(in);
  }
}
