package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.execution.connector.ListConnector;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.UUID;
import lombok.Getter;

public class SourceSlot<OUT> extends Slot {

  private final Source<OUT> source;
  @Getter
  private final ListConnector<OUT> connector;

  public SourceSlot(final Source<OUT> source,
                    final UUID topologyNodeId,
                    final ListConnector<OUT> connector) {
    super(topologyNodeId);
    this.source = source;
    this.connector = connector;
    source.init(connector);
  }

  @Override
  public void runStep() {
    this.source.read();
  }

  @Override
  public void tick() {
    this.connector.tick();
  }
}
