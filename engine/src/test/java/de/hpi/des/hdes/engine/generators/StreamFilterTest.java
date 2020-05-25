package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import de.hpi.des.hdes.engine.generators.FilterGenerator;

public class StreamFilterTest {

    @Test
    void generateModuloFilter() {
        FilterGenerator generator = new FilterGenerator("element % 4 == 0");
        String out = generator.generate("implementation");
        assertEquals("if ( element % 4 == 0 ) { implementation}", out.replaceAll("( ){2,}|\n|\r", ""));
    }
}
