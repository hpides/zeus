package de.hpi.des.hdes.engine.shared.aggregation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.udf.Filter;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SharedSelection<V> extends AbstractTopologyElement<SharedValue<V>>
    implements OneInputOperator<V, SharedValue<V>> {

  private final Node parentNode;
  private final WindowAssigner<? extends Window> sliceAssigner;
  private final Map<Window, QueryChangelog> queryMap;

  public SharedSelection(final Node parentNode,
      final WindowAssigner<? extends Window> sliceAssigner) {
    this.parentNode = parentNode;
    this.sliceAssigner = sliceAssigner;
    this.queryMap = new ConcurrentHashMap<>();
  }

  @Override
  public void process(final AData<V> in) {
    // there is only one window
    final Window window = this.sliceAssigner.assignWindows(in.getEventTime()).get(0);
    final QueryChangelog changeSet = this.queryMap.get(window);
    final List<SharedQuery> queries = changeSet.getQueries();
    final QuerySet querySet = new QuerySet();
    for (int i = 0; i < queries.size(); i++) {
      final SharedQuery query = queries.get(i);
      final Filter<V> filter = query.getFilter(this.parentNode);
      if (filter.filter(in.getValue())) {
        querySet.set(i);
      }
    }
    this.collector.collect(in.transform(new SharedValue<>(in.getValue(), querySet)));
  }

  public void addNewChangelog(final Window window, final QueryChangelog changelog) {
    this.queryMap.put(window, changelog);
  }
}
