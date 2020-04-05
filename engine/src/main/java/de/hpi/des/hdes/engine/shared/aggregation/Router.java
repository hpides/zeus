package de.hpi.des.hdes.engine.shared.aggregation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;

// deployed for each downstream processor and selects only relevant tuple
public class Router<IN> extends AbstractTopologyElement<IN> implements
    OneInputOperator<SharedValue<IN>, IN> {

  private final int queryIndex;

  public Router(final int queryIndex) {
    this.queryIndex = queryIndex;
  }

  @Override
  public void process(final AData<SharedValue<IN>> in) {
    final SharedValue<IN> value = in.getValue();
    if (value.getQuerySet().get(this.queryIndex)) {
      this.collector.collect(in.transform(value.getValue()));
    }
  }
}
