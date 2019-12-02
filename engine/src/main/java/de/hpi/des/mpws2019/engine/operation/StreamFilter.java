package de.hpi.des.mpws2019.engine.operation;

import de.hpi.des.mpws2019.engine.function.Filter;

public class StreamFilter<IN> extends AbstractOperation<IN>
    implements OneInputOperator<IN, IN> {

  private final Filter<? super IN> filter;

  public StreamFilter(final Filter<? super IN> filter) {
    this.filter = filter;
  }

  @Override
  public void process(final IN in) {
    if (this.filter.filter(in)) {
      this.collector.collect(in);
    }
  }

}