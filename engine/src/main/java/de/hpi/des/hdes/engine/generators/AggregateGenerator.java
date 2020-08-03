package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import de.hpi.des.hdes.engine.window.CWindow;
import lombok.Getter;

@Getter
public class AggregateGenerator implements Generatable {

  private final AggregateFunction aggregateFunction;
  private final int aggregateValueIndex;
  private final CWindow window;

  public AggregateGenerator(AggregateFunction aggregateFunction, final int aggregateValueIndex, final CWindow window) {
    this.aggregateFunction = aggregateFunction;
    this.aggregateValueIndex = aggregateValueIndex;
    this.window = window;
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
