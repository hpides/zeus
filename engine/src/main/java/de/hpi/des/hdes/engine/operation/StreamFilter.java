package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Filter;
import org.jetbrains.annotations.NotNull;

public class StreamFilter<IN> extends AbstractTopologyElement<IN>
    implements OneInputOperator<IN, IN> {

  private final Filter<? super IN> filter;

  public StreamFilter(final Filter<? super IN> filter) {
    this.filter = filter;
  }

  @Override
  public void process(@NotNull final IN in) {
    if (this.filter.filter(in)) {
      this.collector.collect(in);
    }
  }

}
