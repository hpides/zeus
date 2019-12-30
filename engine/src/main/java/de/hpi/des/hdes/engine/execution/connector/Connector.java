package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.Collector;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.Getter;

@Getter
public class Connector<VAL> implements Collector<VAL> {

  private final Map<UUID, Consumer> nodeIdToFunction = new ConcurrentHashMap<>();
  private final Map<UUID, Buffer<VAL>> nodeIdToBuffer = new ConcurrentHashMap<>();

  public void addFunction(Node node, final Consumer<VAL> processFunc) {
    nodeIdToFunction.put(node.getNodeId(), processFunc);
  }

  public Buffer<VAL> getBuffer(final Node node) {
    return nodeIdToBuffer.get(node.getNodeId());
  }

  public Buffer<VAL> addBuffer(final Node node) {
    Buffer<VAL> buffer = new QueueBuffer<>();
    nodeIdToBuffer.put(node.getNodeId(), buffer);
    return buffer;
  }

  @Override
  public void collect(final VAL val) {
    for (var queue: nodeIdToBuffer.values()) {
      queue.add(val);
    }
    for (var f: nodeIdToFunction.values()) {
      f.accept(val);
    }
  }
}
