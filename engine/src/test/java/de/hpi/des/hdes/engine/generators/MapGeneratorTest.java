package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import de.hpi.des.hdes.engine.CompiledEngine;
import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.cstream.CStream;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

public class MapGeneratorTest {
  @Test
  void throwFunctionErrorMap() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_, _) 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    assertThatThrownBy(() -> generator.generate(l)).hasMessageContaining("Malformatted function");
  }

  @Test
  void throwParameterErrorMap() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_) -> 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    assertThatThrownBy(() -> generator.generate(l)).hasMessageContaining("Malformatted parameters");
  }

  @Test
  void generateAddingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    MapGenerator generator = new MapGenerator(t.add(PrimitiveType.LONG, "(_, _) -> 3"));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l);
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
    String out = generator.generate(l);
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
    String out = generator.generate(l);
    assertThat(out).isEmpty();
    assertThat(l.getCurrentTypes()).containsExactly("$0");
  }

  @Test
  void generateRemovingMapper() {
    Tuple t = new Tuple(PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG);
    MapGenerator generator = new MapGenerator(t.remove(1));
    UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(t.getTypes(), t.getNextTuple().getTypes(), generator));
    String out = generator.generate(l);
    assertThat(out).isEmpty();
    assertThat(l.getCurrentTypes()).containsExactly(null, "$0", "$1");
  }

  @Test
  void mapperTest() {
    JobManager manager = new JobManager(new CompiledEngine());
    VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

    CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, "generatorHost",
        1);
    builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, "generatorHost", 2)
        .filter(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
            "(v1, _, _, _) -> v1 > 100")
        .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
            new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT })
                .add(PrimitiveType.LONG, "(_,_,_,_) -> System.currentTimeMillis()"))
        .join(sourceOne, new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
            new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, 0, 0, 1000)
        .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
            PrimitiveType.LONG }, 1000);

    manager.addQuery(builder.buildAsQuery());
  }
}
