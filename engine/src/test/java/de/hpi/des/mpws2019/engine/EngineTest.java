package de.hpi.des.mpws2019.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.operation.ListSink;
import de.hpi.des.mpws2019.engine.operation.ListSource;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EngineTest {

  @Test
  void testRun() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final ListSource<Integer> source = new ListSource<>(list);
    final ListSource<String> stringSource = new ListSource<>(
        List.of("2", "5", "4", "3"));

    final ListSink<String> sink = new ListSink<>(new ArrayList<>());

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    var engine = new Engine(builder);
    engine.run();
    while (!source.isDone() && !stringSource.isDone()) {
      sleep(100);
    }
    assertThat(sink.getList()).containsExactly("22", "44", "33");
  }
}