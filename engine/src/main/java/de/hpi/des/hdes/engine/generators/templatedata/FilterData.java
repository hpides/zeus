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
  final private String freeFunction;
  final private String inputName;
  final private int tupleLength;

  public FilterData(Pipeline pipeline, PrimitiveType[] types, String condition, boolean isRight, String freeFunction,
      String inputName) {
    LambdaString lambda = LambdaString.analyze(types, condition);
    this.condition = lambda.getExecution();
    this.signature = lambda.getSignature();
    // Registers the variables fo materialization
    this.application = Arrays.stream(lambda.getMaterializationData()).mapToObj(m -> {
      MaterializationData var = ((BinaryPipeline) pipeline).getVariableAtIndex(m, isRight);
      return var.getVarName();
    }).collect(Collectors.joining(", "));
    // Registers the interface on the generator
    this.interfaceName = pipeline.registerInterface("boolean", lambda.getInterfaceDef()).getInterfaceName();
    this.freeFunction = freeFunction;
    this.inputName = inputName;
    this.tupleLength = Arrays.stream(types).mapToInt(t -> t.getLength()).sum();
  }

  public FilterData(Pipeline pipeline, PrimitiveType[] types, String condition, String freeFunction, String inputName) {
    LambdaString lambda = LambdaString.analyze(types, condition);
    this.condition = lambda.getExecution();
    this.signature = lambda.getSignature();
    // Registers the variables fo materialization
    this.application = Arrays.stream(lambda.getMaterializationData()).mapToObj(m -> {
      MaterializationData var = pipeline.getVariableAtIndex(m);
      return var.getVarName();
    }).collect(Collectors.joining(", "));
    // Registers the interface on the generator
    this.interfaceName = pipeline.registerInterface("boolean", lambda.getInterfaceDef()).getInterfaceName();
    this.freeFunction = freeFunction;
    this.inputName = inputName;
    this.tupleLength = Arrays.stream(types).mapToInt(t -> t.getLength()).sum();
  }
}
