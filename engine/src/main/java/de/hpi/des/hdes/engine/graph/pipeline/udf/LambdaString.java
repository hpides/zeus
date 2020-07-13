package de.hpi.des.hdes.engine.graph.pipeline.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class LambdaString {
  final private PrimitiveType[] interfaceDef;
  final private int[] materializationData;
  final private String execution;
  final private String signature;

  public static LambdaString analyze(PrimitiveType[] types, String lambda) {
    // Split at arrow
    String[] functionSplit = lambda.split("->");
    // Check if function conforms to template
    if (functionSplit.length != 2)
      throw new Error("Malformatted function");
    // Get all parameters
    String[] parameters = functionSplit[0].replaceAll("[( )]", "").split(",");
    // Check if enough parameters are defined
    if (parameters.length != types.length)
      throw new Error("Malformatted parameters");
    // Creates an array of materialization data, parameter types, and parameter
    // names
    List<Integer> mat = new ArrayList<>();
    List<PrimitiveType> interfaceTypes = new ArrayList<>();
    List<String> params = new ArrayList<>();
    // Offset for deserialization of values
    for (int i = 0; i < parameters.length; i++) {
      if (!parameters[i].equals("_") && functionSplit[1].contains(parameters[i])) {
        // The parameter gets used
        mat.add(i);
        // adding type to generated interface
        interfaceTypes.add(types[i]);
        // adding parameter to signature
        params.add(parameters[i]);
      }
    }
    return new LambdaString(interfaceTypes.toArray(new PrimitiveType[interfaceTypes.size()]),
        mat.stream().mapToInt(i -> i.intValue()).toArray(), functionSplit[1],
        params.stream().collect(Collectors.joining(", ")));
  }
}