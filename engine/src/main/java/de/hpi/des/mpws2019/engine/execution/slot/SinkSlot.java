package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.Collector;
import de.hpi.des.mpws2019.engine.operation.Sink;

public class SinkSlot<IN> extends Slot {

  private final Sink<IN> sink;
  private final InputBuffer<IN> input;
  private final Collector<IN> collector;

  public SinkSlot(final Sink<IN> sink, final InputBuffer<IN> input) {
    this.sink = sink;
    this.collector = sink.getCollector();
    this.input = input;
  }


  public void run() {
    final IN in = this.input.poll();
    if (in != null) {
      this.collector.collect(in);
    }
  }
}
