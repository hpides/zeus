package de.hpi.des.hdes.engine.graalvm;

import java.io.IOException;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.graalvm.execution.operation.StreamFilter;
import de.hpi.des.hdes.engine.AData;

import java.lang.Integer;
import java.util.LinkedList;
import java.util.List;

public class GraalVMProcessorTest {

    @Test
    void shouldFilterElementsNative() throws IOException {
        final List<Integer> result = new LinkedList<>();
        final StreamFilter<Integer> filter = new StreamFilter<>(i -> i % 2 == 0);
        filter.init(e -> result.add(e.getValue()));

        List.of(-2, 5, 0, 1, 3, 10, 2, 15)
        .forEach(e -> filter.process(AData.of(e)));

        assertThat(result).containsExactly(-2, 0, 10, 2);
    }

}