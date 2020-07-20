package de.hpi.des.hdes.engine.generators.templatedata;

import java.util.Arrays;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.udf.LambdaString;
import lombok.Getter;

@Getter
public class FilterData {
  final private String interfaceName;
  final private String signature;
  final private String condition;
  final private String application;

  public FilterData(Pipeline pipeline, PrimitiveType[] types, String condition, boolean isRight) {
    LambdaString lambda = LambdaString.analyze(types, condition);
    this.condition = lambda.getExecution();
    this.signature = lambda.getSignature();
    // Registers the variables fo materialization
    this.application = Arrays.stream(lambda.getMaterializationData()).mapToObj(m -> {
      MaterializationData var = isRight ? ((BinaryPipeline) pipeline).getVariableAtIndex(m, isRight)
          : pipeline.getVariableAtIndex(m);
      return var.getVarName();
    }).collect(Collectors.joining(", "));
    // Registers the interface on the generator
    this.interfaceName = pipeline.registerInterface("boolean", lambda.getInterfaceDef()).getInterfaceName();
  }
}
