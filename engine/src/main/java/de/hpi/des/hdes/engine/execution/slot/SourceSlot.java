package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.Source;

public class SourceSlot<OUT> extends Slot {

  private final Source<OUT> source;

  public SourceSlot(final Source<OUT> source,
                    final Collector<OUT> outOutput) {
    this.source = source;
    source.init(outOutput);
  }

  @Override
  public void runStep() {
    this.source.read();
  }
}
