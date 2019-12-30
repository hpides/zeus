package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.execution.connector.Connector;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

public class SourceSlot<OUT> extends Slot {

  private final Source<OUT> source;
  @Getter
  private final Collector<OUT> collector;
  @Getter
  private final Connector connector;

  public SourceSlot(final Source<OUT> source,
                    final Collector<OUT> outOutput,
                    final UUID topologyNodeId,
                    final Connector connector) {
    super(topologyNodeId);
    this.source = source;
    this.collector = outOutput;
    this.connector = connector;
    source.init(outOutput);
  }

  @Override
  public void runStep() {
    this.source.read();
  }
}
