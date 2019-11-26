package de.hpi.des.mpws2019.engine.execution.slot;

import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.operation.Collector;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

public class QueueConnector<VAL> implements Collector<VAL> {

  @Getter
  private final Map<UUID, QueueBuffer<VAL>> nodeIdToQueueBuffer = new HashMap<>();

  public QueueBuffer<VAL> getQueueBuffer(final Node node) {
    return nodeIdToQueueBuffer.get(node.getNodeId());
  }

  public QueueBuffer<VAL> addQueueBuffer(final Node node) {
    QueueBuffer<VAL> queueBuffer = new QueueBuffer<>();
    nodeIdToQueueBuffer.put(node.getNodeId(), queueBuffer);
    return queueBuffer;
  }

  @Override
  public void collect(final VAL val) {
    for (var queue: nodeIdToQueueBuffer.values()) {
      queue.add(val);
    }
  }
}
