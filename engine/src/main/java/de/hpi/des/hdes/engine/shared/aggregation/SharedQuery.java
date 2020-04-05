package de.hpi.des.hdes.engine.shared.aggregation;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.Filter;
import java.util.Map;

public class SharedQuery extends Query {

  private final long creationTimestamp;
  private Map<Node, Filter<?>> filterMap;
  private Map<Node, Aggregator<?, ?, ?>> aggregatorMap;

  public SharedQuery(final Topology topology) {
    super(topology);
    this.creationTimestamp = System.nanoTime();
  }

  public long getCreationTimestamp() {
    return this.creationTimestamp;
  }

  <V> Filter<V> getFilter(final Node node) {
    return (Filter<V>) this.filterMap.get(node);
  }

  <IN, STATE, OUT> Aggregator<IN, STATE, OUT> getAggregator(final Node node) {
    return (Aggregator<IN, STATE, OUT>) aggregatorMap.get(node);
  }
}
