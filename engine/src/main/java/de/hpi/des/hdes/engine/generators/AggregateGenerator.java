package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import lombok.Getter;

public class AggregateGenerator implements Generatable {

  private final StringWriter writer = new StringWriter();

  @Getter
  private final AggregateFunction aggregateFunction;
  @Getter
  private final int aggregateValueIndex;

  public AggregateGenerator(AggregateFunction aggregateFunction, final int aggregateValueIndex) {
    this.aggregateFunction = aggregateFunction;
    this.aggregateValueIndex = aggregateValueIndex;
  }

  @Override
  public String generate(Pipeline pipeline, String execution) {
    // TODO Auto-generated method stub
    return null;
  }

}