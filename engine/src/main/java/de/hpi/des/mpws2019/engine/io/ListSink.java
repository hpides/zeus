package de.hpi.des.mpws2019.engine.io;

import de.hpi.des.mpws2019.engine.operation.Collector;
import de.hpi.des.mpws2019.engine.operation.Sink;
import java.util.List;
import lombok.Getter;

@Getter
public class ListSink<IN> implements Sink<IN> {

  private Collector<IN> collector;

  public ListSink(List<IN> list) {
    this.collector = list::add;
  }

  @Override
  public void process(final IN in) {
    this.collector.collect(in);
  }
}
