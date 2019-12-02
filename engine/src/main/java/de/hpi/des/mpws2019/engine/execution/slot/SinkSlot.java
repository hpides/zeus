package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.execution.connector.Buffer;
import de.hpi.des.mpws2019.engine.operation.Sink;

public class SinkSlot<IN> extends Slot {

  private final Buffer<IN> input;
  private final Sink<IN> sink;

  public SinkSlot(final Sink<IN> sink, final Buffer<IN> input) {
    this.sink = sink;
    this.input = input;
  }


  public void runStep() {
    final IN in = this.input.poll();
    if (in != null) {
      this.sink.process(in);
    }
  }
}
