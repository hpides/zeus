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
    void generateFilter() {
        FilterGenerator generator = new FilterGenerator(new PrimitiveType[]{PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT}, "(v1, v2, v3) -> v1 > 1");
        String out = generator.generate("implementation");
        assertThat(out).containsPattern(Pattern.compile("long v1 = input.getLong()", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("(long v1) -> v1>1", Pattern.LITERAL));
        assertThat(out).containsPattern(Pattern.compile("if(filter.apply(v1))", Pattern.LITERAL));
    }
}
