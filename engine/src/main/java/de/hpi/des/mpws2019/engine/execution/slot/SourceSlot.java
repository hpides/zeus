package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.operation.Collector;
import de.hpi.des.mpws2019.engine.operation.Source;

public class SourceSlot<OUT> extends Slot {

  private final Source<OUT> source;

  public SourceSlot(final Source<OUT> source,
                    final Collector<OUT> outOutput) {
    this.source = source;
    source.init(outOutput);
  }

  @Override
  public void run() {
    this.source.read();
  }
}
