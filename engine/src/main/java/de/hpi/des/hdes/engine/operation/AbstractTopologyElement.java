package de.hpi.des.hdes.engine.operation;


import de.hpi.des.hdes.engine.Query;
import java.util.ArrayList;
import lombok.Getter;

public abstract class AbstractTopologyElement<OUT> implements Initializable<OUT> {

  protected Collector<OUT> collector = null;

  @Getter
  private ArrayList<Query> associatedQueries = new ArrayList<>();

  @Override
  public void init(final Collector<OUT> collector) {
    this.collector = collector;
  }

  public void tick() {
    this.collector.tick();
  }

  public void addAssociatedQuery(Query query) {
    this.associatedQueries.add(query);
  }
}
