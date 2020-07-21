package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import lombok.Getter;

public class AggregateGenerator implements Generatable {

  @Getter
  private final AggregateFunction aggregateFunction;
  @Getter
  private final int aggregateValueIndex;
  @Getter
  private final int windowLength;

  public AggregateGenerator(AggregateFunction aggregateFunction, final int aggregateValueIndex, final int windowLength) {
    this.aggregateFunction = aggregateFunction;
    this.aggregateValueIndex = aggregateValueIndex;
    this.windowLength = windowLength;
  }

  @Override
  public String generate(Pipeline pipeline) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getOperatorId() {
    return aggregateFunction.name().concat(Integer.toString(aggregateValueIndex));
  }

}
