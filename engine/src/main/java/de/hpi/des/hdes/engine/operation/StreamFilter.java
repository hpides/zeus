package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Filter;

/**
 * Operator that filters elements based on a predicate
 *
 * @param <IN> the type of the input elements
 */
public class StreamFilter<IN> extends AbstractTopologyElement<IN>
    implements OneInputOperator<IN, IN> {

  private final Filter<? super IN> filter;

  public StreamFilter(final Filter<? super IN> filter) {
    this.filter = filter;
  }

  @Override
  public void process(final AData<IN> aData) {
    if (this.filter.filter(aData.getValue())) {
      this.collector.collect(aData);
    }
  }

}
