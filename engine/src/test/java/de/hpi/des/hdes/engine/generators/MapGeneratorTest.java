package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;

public class MapGeneratorTest {
  @Test
  void throwFunctionErrorMap() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_, _) 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    assertThatThrownBy(() -> generator.generate(l, "implementation")).hasMessageContaining("Malformatted function");
  }

  @Test
  void throwParameterErrorMap() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_) -> 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    assertThatThrownBy(() -> generator.generate(l, "implementation")).hasMessageContaining("Malformatted parameters");
  }

  @Test
  void generateAddingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_, _) -> 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l, "implementation");
    assertThat(out).contains("map = () ->  3;");
    assertThat(out).contains("$0");
    assertThat(out).contains("map.apply()");
    assertThat(l.getCurrentTypes()).containsExactly(null, null, "$0");
  }

  @Test
  void generateMutatingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.mutateAt(1, PrimitiveType.LONG, "(_, _) -> 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l, "implementation");
    assertThat(out).contains("map = () ->  3;");
    assertThat(out).contains("$0");
    assertThat(out).contains("map.apply()");
    assertThat(l.getCurrentTypes()).containsExactly(null, "$0");
  }

  @Test
  void generateGettingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.get(1));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l, "implementation");
    assertThat(out).isEmpty();
    assertThat(l.getCurrentTypes()).containsExactly("$0");
  }

  @Test
  void generateRemovingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG);
    MapGenerator generator = new MapGenerator(t.remove(1));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l, "implementation");
    assertThat(out).isEmpty();
    assertThat(l.getCurrentTypes()).containsExactly(null, "$0", "$1");
  }
}