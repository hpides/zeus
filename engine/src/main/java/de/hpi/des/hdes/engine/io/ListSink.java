package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.Sink;
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
