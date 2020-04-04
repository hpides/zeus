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

/**
 * A slot wraps an operator and executes it. The output of the operator is routed to the outputs.
 *
 * @param <OUT> output type of the wrapped operator
 */
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

  /**
   * Adds a operator to route the output elements of the operator to.
   *
   * @param node
   * @param processFunc downstream oneinput operator
   */
  public void addOutput(final Node node, final OneInputOperator<OUT, ?> processFunc) {
    log.debug("Add func {} for node {} in slot {}", processFunc, node, this);
    this.addDownstreamElement(node, processFunc);
  }

  /**
   * Adds a sink to route the output elements of the operator to.
   *
   * @param node /TODO what is this
   * @param sink downstream sink
   */
  public void addOutput(final Node node, final Sink<OUT> sink) {
    log.debug("Add sink {} for node {} in slot {}", sink, node, this);
    this.addDownstreamElement(node, sink);
  }

  /**
   * Adds a buffer to route the output elements of the operator to.
   *
   * @param node
   * @param buffer downstream buffer
   */
  public void addOutput(final Node node, final Buffer<AData<OUT>> buffer) {
    log.debug("Add buffer {} for node {} in slot {}", buffer, node, this);
    this.addDownstreamElement(node, buffer);
    this.buffers.add(buffer);
  }

  // TODO comment
  private void addDownstreamElement(final Node node, final SlotProcessor<AData<OUT>> buffer) {
    this.processorMap.put(node, buffer);
    this.processors.add(buffer);
  }

  /**
   * Collects output of an operator and sends them downstream.
   *
   * @param event collected output event
   */
  @Override
  public void collect(final AData<OUT> event) {
    for (final SlotProcessor<AData<OUT>> processor : this.processors) {
      processor.sendDownstream(event);
    }
  }

  /**
   * Add a downstream slot to the {@link Slot#children} build a slotgraph.
   *
   * @param slot child slot
   */
  public void addChild(final Slot<?> slot) {
    this.children.add(slot);
  }

  // TODO comment
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


  /**
   * Marks whether the slot is shutdown
   *
   * @return true if shutdown
   */
  public boolean isShutdown() {
    return this.hasNoOutput();
  }

  /**
   * {@inheritDoc}
   *
   * Flushes underlying slot buffer.
   */
  @Override
  public void tick() {
    for (final var buffer : this.buffers) {
      buffer.flushIfTimeout();
    }
  }

  /**
   * Marks whether the slot has any children.
   *
   * @return true if there are no more children
   */
  protected boolean hasNoOutput() {
    return this.processors.isEmpty();
  }
}
