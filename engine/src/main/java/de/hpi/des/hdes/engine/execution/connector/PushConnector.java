package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.Collector;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.Getter;

public class PushConnector<VAL> implements Collector<VAL> {

  @Getter
  private final Map<UUID, Consumer> nodeIdToFunction = new HashMap<>();

  public void addFunction(Node node, final Consumer<VAL> processFunc) {
    nodeIdToFunction.put(node.getNodeId(), processFunc);
  }

  @Override
  public void collect(final VAL val) {
    for (var f: nodeIdToFunction.values()) {
      f.accept(val);
    }
  }
}
