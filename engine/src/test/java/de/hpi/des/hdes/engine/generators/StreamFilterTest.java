package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.regex.Pattern;

public class StreamFilterTest {

    @Test
    void throwFunctionErrorFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[0],"v1 > 1");
        assertThatThrownBy(() -> generator.generate("implementation")).hasMessageContaining("Malformatted function");
    }

    @Test
    void throwParameterErrorFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[0],"(v1, v2, v3) -> v1 > 1");
        assertThatThrownBy(() -> generator.generate("implementation")).hasMessageContaining("Malformatted parameters");
    }

    @Test
    void generateOneArgumentFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(v1, v2, v3) -> v1 > 1");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("input.position(input.position() + 0)", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("long v1 = input.getLong()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("(long $v1) ->  $v1 > 1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply(v1))", Pattern.LITERAL));
    }

    @Test
    void generateTwoArgumentFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(v1, v2, v3) -> v1 > 1 && v2 > v1");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("input.position(input.position() + 0)", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("long v1 = input.getLong()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("int v2 = input.getInt()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("(long $v1, int $v2) ->  $v1 > 1 && $v2 > $v1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply(v1, v2))", Pattern.LITERAL));
    }
    
    @Test
    void generateTwoGappingArgumentFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(v1, v2, v3) -> v1 > 1 && v3 > v1");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("input.position(input.position() + 0)", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("long v1 = input.getLong()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("input.position(input.position() + 4)", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("int v3 = input.getInt()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("(long $v1, int $v3) ->  $v1 > 1 && $v3 > $v1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply(v1, v3))", Pattern.LITERAL));
    }

    
    @Test
    void generateNoArgumentFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(v1, v2, v3) -> true");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("() ->  true", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply())", Pattern.LITERAL));
    }

    @Test
    void generateSkippingArgumentFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(_, _, _) -> true");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("() ->  true", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply())", Pattern.LITERAL));
    }
}
