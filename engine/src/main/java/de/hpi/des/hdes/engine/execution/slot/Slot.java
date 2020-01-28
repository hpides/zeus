package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class Slot<OUT> implements Collector<OUT> {

  private final Map<Node, OneInputOperator<OUT, ?>> outOps = new ConcurrentHashMap<>();
  private final Map<Node, Sink<OUT>> outSinks = new ConcurrentHashMap<>();
  private final Map<Node, Buffer<OUT>> outBuffer = new ConcurrentHashMap<>();
  private final List<Slot<?>> children = new CopyOnWriteArrayList<>();
  private final List<Map<Node, ?>> outputs = List.of(this.outOps, this.outSinks, this.outBuffer);

  /**
   * Retrieve associated topology node.
   */
  public abstract Node getTopologyNode();

  public void addOutput(final Node node, final OneInputOperator<OUT, ?> processFunc) {
    log.debug("Add func {} for node {} in slot {}", processFunc, node, this);
    this.outOps.put(node, processFunc);
  }

  public void addOutput(final Node node, final Sink<OUT> sink) {
    log.debug("Add sink {} for node {} in slot {}", sink, node, this);
    this.outSinks.put(node, sink);
  }

  public void addOutput(final Node node, final Buffer<OUT> buffer) {
    log.debug("Add buffer {} for node {} in slot {}", buffer, node, this);
    this.outBuffer.put(node, buffer);
  }

  @Override
  public void collect(final AData<OUT> event) {
    for (final var op : this.outOps.values()) {
      log.trace("{}: Output {} to {}", this, event, op);
      op.process(event);
    }
    for (final var sink : this.outSinks.values()) {
      log.trace("{}: Output {} to {}", this, event, sink);
      sink.process(event);
    }
    for (final var buffer : this.outBuffer.values()) {
      log.trace("{}: Output {} to {}", this, event, buffer);
      buffer.add(event);
    }
  }

  public void addChild(final Slot<?> slot) {
    this.children.add(slot);
  }

  public void remove(final Query query) {
    for (final Map<Node, ?> nodeMap : this.outputs) {
      for (final Node node : nodeMap.keySet()) {
        log.debug("Remove query {} for node {}", query, node);
        node.removeAssociatedQuery(query);
        if (node.getAssociatedQueries().isEmpty()) {
          log.debug("Remove output for node {} from slot {}", node, this);
          nodeMap.remove(node);
        }
      }
    }
  }

  public boolean isShutdown() {
    return this.hasNoOutput();
  }

  @Override
  public void tick() {
    for (final var buffer : this.outBuffer.values()) {
      buffer.flushIfTimeout();
    }
  }

  public List<Slot<?>> getChildren() {
    return this.children;
  }

  public List<Map<Node, ?>> getOutputs() {
    return this.outputs;
  }

  private boolean hasNoOutput() {
    return this.getOutputs().stream().allMatch(Map::isEmpty);
  }
}
