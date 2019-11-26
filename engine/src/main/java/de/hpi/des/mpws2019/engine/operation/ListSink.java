package de.hpi.des.mpws2019.engine.operation;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ListSink<IN> extends AbstractOperation<Void> implements Sink<IN> {
  private final List<IN> list;

  @Override
  public void process(final IN in) {
    this.list.add(in);
  }
}
