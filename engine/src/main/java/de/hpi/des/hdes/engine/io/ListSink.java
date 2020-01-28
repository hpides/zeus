package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.List;
import lombok.Getter;

@Getter
public class ListSink<IN> implements Sink<IN> {

  private final Collector<IN> collector;

  public ListSink(final List<IN> list) {
    this.collector = e -> list.add(e.getValue());
  }

  @Override
  public void process(final AData<IN> in) {
    this.collector.collect(in);
  }
}
