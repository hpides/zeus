package de.hpi.des.hdes.engine.graph.pipeline.udf;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.generators.PrimitiveType;

import static org.assertj.core.api.Assertions.*;

public class LambdaStringTest {
  @Test
  void throwFunctionError() {
    assertThatThrownBy(() -> LambdaString.analyze(new PrimitiveType[0], "v1 > 1"))
        .hasMessageContaining("Malformatted function");
  }

  @Test
  void throwParameterError() {
    assertThatThrownBy(() -> LambdaString.analyze(new PrimitiveType[0], "(v1, v2, v3) -> v1 > 1"))
        .hasMessageContaining("Malformatted parameters");
  }

  @Test
  void generateOneArgument() {
    LambdaString l = LambdaString.analyze(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT }, "(v1, v2, v3) -> v1 > 1");
    assertThat(l.getExecution()).contains("v1 > 1");
    assertThat(l.getSignature()).isEqualTo("v1");
    assertThat(l.getInterfaceDef()).containsExactly(PrimitiveType.LONG);
    assertThat(l.getMaterializationData()).containsExactly(0);
  }

  @Test
  void generateTwoArgument() {
    LambdaString l = LambdaString.analyze(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
        "(v1, v2, v3) -> v1 > 1 && v2 > v1");
    assertThat(l.getExecution()).contains("v1 > 1 && v2 > v1");
    assertThat(l.getSignature()).isEqualTo("v1, v2");
    assertThat(l.getInterfaceDef()).containsExactly(PrimitiveType.LONG, PrimitiveType.INT);
    assertThat(l.getMaterializationData()).containsExactly(0, 1);
  }

  @Test
  void generateTwoGappingArgument() {
    LambdaString l = LambdaString.analyze(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
        "(v1, v2, v3) -> v1 > 1 && v3 > v1");
    assertThat(l.getExecution()).contains("v1 > 1 && v3 > v1");
    assertThat(l.getSignature()).isEqualTo("v1, v3");
    assertThat(l.getInterfaceDef()).containsExactly(PrimitiveType.LONG, PrimitiveType.INT);
    assertThat(l.getMaterializationData()).containsExactly(0, 2);
  }

  @Test
  void generateNoArgument() {
    LambdaString l = LambdaString.analyze(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT }, "(v1, v2, v3) -> true");
    assertThat(l.getExecution()).contains("true");
    assertThat(l.getSignature()).isEqualTo("");
    assertThat(l.getInterfaceDef()).isEmpty();
    assertThat(l.getMaterializationData()).isEmpty();
  }

  @Test
  void generateSkippingArgumentFilter() {
    LambdaString l = LambdaString
        .analyze(new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT }, "(_, _, _) -> true");
    assertThat(l.getExecution()).contains("true");
    assertThat(l.getSignature()).isEqualTo("");
    assertThat(l.getInterfaceDef()).isEmpty();
    assertThat(l.getMaterializationData()).isEmpty();
  }
}