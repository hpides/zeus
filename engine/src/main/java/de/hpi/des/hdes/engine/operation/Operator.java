package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.Query;
import java.util.ArrayList;

public interface Operator<OUT> extends Initializable<OUT> {
  void tick();
  void addAssociatedQuery(Query query);
  ArrayList<Query> getAssociatedQueries();

}
