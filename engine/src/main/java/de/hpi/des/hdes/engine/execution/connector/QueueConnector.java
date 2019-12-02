package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.Collector;
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
