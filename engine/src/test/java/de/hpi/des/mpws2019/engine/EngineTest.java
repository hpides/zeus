package de.hpi.des.mpws2019.engine;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.sink.CollectionSink;
import de.hpi.des.mpws2019.engine.source.CollectionSource;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;

class EngineTest {

  @Test
  void testEngine() {
    final Function<Integer, Integer> mapper = i -> i + 1;
    final List<Integer> input = List.of(1, 2, 3, 4, 5);
    final List<Integer> expectedOutput = Seq.seq(input)
        .map(mapper)
        .toList();
    final CollectionSink<Integer> sink = new CollectionSink<>();
    final Engine<Integer> engine = new Engine<>(
        new CollectionSource<>(input),
        sink,
        mapper,
        Executors.newSingleThreadExecutor()
    );
    engine.start();
    while (true) {
      if (sink.getCollection().size() == expectedOutput.size()) {
        engine.shutdown();
        break;
      }
    }
    assertThat(sink.getCollection()).containsExactlyElementsOf(expectedOutput);
  }
}