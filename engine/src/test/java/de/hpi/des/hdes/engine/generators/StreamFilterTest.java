package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import de.hpi.des.hdes.engine.generators.FilterGenerator;

public class StreamFilterTest {

    @Test
    void generateModuloFilter() {
        FilterGenerator<Integer> generator = new FilterGenerator<Integer>("element % 4 == 0");
        String out = generator.generate();
        assertEquals("if ( element % 4 == 0 ) { %s }", out);
    }
}