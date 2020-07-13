package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;

import static org.assertj.core.api.Assertions.*;

import java.util.regex.Pattern;

public class FilterGeneratorTest {

    @Test
    void throwFunctionErrorFilter() {
        PrimitiveType[] types = new PrimitiveType[0];
        FilterGenerator generator = new FilterGenerator(types, "v1 > 1");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        assertThatThrownBy(() -> generator.generate(l, "implementation")).hasMessageContaining("Malformatted function");
    }

    @Test
    void throwParameterErrorFilter() {
        PrimitiveType[] types = new PrimitiveType[0];
        FilterGenerator generator = new FilterGenerator(types, "(v1, v2, v3) -> v1 > 1");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        assertThatThrownBy(() -> generator.generate(l, "implementation"))
                .hasMessageContaining("Malformatted parameters");
    }

    @Test
    void generateOneArgumentFilter() {
        PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT };
        FilterGenerator generator = new FilterGenerator(types, "(v1, v2, v3) -> v1 > 1");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        String out = generator.generate(l, "implementation");
        assertThat(out).containsPattern(Pattern.compile("(v1) ->  v1 > 1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("filter.apply($0))", Pattern.LITERAL));
        assertThat(l.getCurrentTypes()).containsExactly("$0", null, null);
    }

    @Test
    void generateTwoArgumentFilter() {
        PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT };
        FilterGenerator generator = new FilterGenerator(types, "(v1, v2, v3) -> v1 > 1 && v2 > v1");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        String out = generator.generate(l, "implementation");
        assertThat(out).containsPattern(Pattern.compile("(v1, v2) ->  v1 > 1 && v2 > v1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("filter.apply($0, $1))", Pattern.LITERAL));
        assertThat(l.getCurrentTypes()).containsExactly("$0", "$1", null);
    }

    @Test
    void generateTwoGappingArgumentFilter() {
        PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT };
        FilterGenerator generator = new FilterGenerator(types, "(v1, v2, v3) -> v1 > 1 && v3 > v1");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        String out = generator.generate(l, "implementation");
        assertThat(out).containsPattern(Pattern.compile("(v1, v3) ->  v1 > 1 && v3 > v1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("filter.apply($0, $1))", Pattern.LITERAL));
        assertThat(l.getCurrentTypes()).containsExactly("$0", null, "$1");
    }

    @Test
    void generateNoArgumentFilter() {
        PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT };
        FilterGenerator generator = new FilterGenerator(types, "(v1, v2, v3) -> true");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        String out = generator.generate(l, "implementation");
        assertThat(out).containsPattern(Pattern.compile("() ->  true", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("filter.apply())", Pattern.LITERAL));
        assertThat(l.getCurrentTypes()).containsExactly(null, null, null);
    }

    @Test
    void generateSkippingArgumentFilter() {
        PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT };
        FilterGenerator generator = new FilterGenerator(types, "(_, _, _) -> true");
        UnaryPipeline l = new UnaryPipeline(new UnaryGenerationNode(types, types, generator));
        String out = generator.generate(l, "implementation");
        assertThat(out).containsPattern(Pattern.compile("() ->  true", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("filter.apply())", Pattern.LITERAL));
        assertThat(l.getCurrentTypes()).containsExactly(null, null, null);
    }
}
