package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.SlotProcessor;
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

  private final Map<Node, SlotProcessor<AData<OUT>>> processorMap = new ConcurrentHashMap<>();
  private final List<SlotProcessor<AData<OUT>>> processors = new CopyOnWriteArrayList<>();
  private final List<Buffer<AData<OUT>>> buffers = new CopyOnWriteArrayList<>();
  private final List<Slot<?>> children = new CopyOnWriteArrayList<>();

  /**
   * Retrieve associated topology node.
   */
  public abstract Node getTopologyNode();

  public void addOutput(final Node node, final OneInputOperator<OUT, ?> processFunc) {
    log.debug("Add func {} for node {} in slot {}", processFunc, node, this);
    this.addDownstreamElement(node, processFunc);
  }

  public void addOutput(final Node node, final Sink<OUT> sink) {
    log.debug("Add sink {} for node {} in slot {}", sink, node, this);
    this.addDownstreamElement(node, sink);
  }

  public void addOutput(final Node node, final Buffer<AData<OUT>> buffer) {
    log.debug("Add buffer {} for node {} in slot {}", buffer, node, this);
    this.addDownstreamElement(node, buffer);
    this.buffers.add(buffer);
  }

  private void addDownstreamElement(final Node node, final SlotProcessor<AData<OUT>> buffer) {
    this.processorMap.put(node, buffer);
    this.processors.add(buffer);
  }

  @Override
  public void collect(final AData<OUT> event) {
    for (final SlotProcessor<AData<OUT>> processor : this.processors) {
      processor.sendDownstream(event);
    }
  }

  public void addChild(final Slot<?> slot) {
    this.children.add(slot);
  }

  public void remove(final Query query) {
    for (final Node node : this.processorMap.keySet()) {
      log.debug("Remove query {} for node {}", query, node);
      node.removeAssociatedQuery(query);
      if (node.getAssociatedQueries().isEmpty()) {
        log.debug("Remove output for node {} from slot {}", node, this);
        final Object processor = this.processorMap.remove(node);
        this.processors.remove(processor);
      }
    }
  }

  public boolean isShutdown() {
    return this.hasNoOutput();
  }

  @Override
  public void tick() {
    for (final var buffer : this.buffers) {
      buffer.flushIfTimeout();
    }
  }

  protected boolean hasNoOutput() {
    return this.processors.isEmpty();
  }
}
