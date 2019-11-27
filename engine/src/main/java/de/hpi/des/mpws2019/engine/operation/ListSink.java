package de.hpi.des.mpws2019.engine.operation;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
public class ListSink<IN> implements Sink<IN> {

  private Collector<IN> collector;

  // TODO: Change interfaces. The user has to set the collector for the sink.
  @Override
  public void init(Collector<IN> collector) {

  }

  public ListSink(List<IN> list) {
    this.collector = list::add;
  }

  @Override
  public void process(final IN in) {
    this.collector.collect(in);
  }
}
