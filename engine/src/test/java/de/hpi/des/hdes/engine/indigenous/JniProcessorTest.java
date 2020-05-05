package de.hpi.des.hdes.engine.indigenous;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.indigenous.execution.operation.StreamFilter;
import de.hpi.des.hdes.engine.indigenous.JniProcessor;
import de.hpi.des.hdes.engine.AData;

import java.io.IOException;
import java.lang.Integer;
import java.util.LinkedList;
import java.util.List;

public class JniProcessorTest {
    
    @Test
    void generateFilterNative() throws IOException, InterruptedException {
        final List<Integer> result = new LinkedList<>();
        final StreamFilter<Integer> filter = new StreamFilter<>(i -> i % 2 == 0);
        JniProcessor.generate(filter);
        JniProcessor.compile(filter);

        filter.init(e -> result.add(e.getValue()));

        List.of(-2, 5, 0, 1, 3, 10, 2, 15)
        .forEach(e -> filter.process(AData.of(e)));

        assertThat(result).containsExactly(-2, 0, 10, 2);
    }

    @Test
    void shouldFilterElementsNative() {
        final List<Integer> result = new LinkedList<>();
        final StreamFilter<Integer> filter = new StreamFilter<>(i -> i % 2 == 0);
        filter.init(e -> result.add(e.getValue()));

        List.of(-2, 5, 0, 1, 3, 10, 2, 15)
        .forEach(e -> filter.process(AData.of(e)));

        assertThat(result).containsExactly(-2, 0, 10, 2);
    }
}