package de.hpi.des.mpws2019.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.io.ListSink;
import de.hpi.des.mpws2019.engine.io.ListSource;
import de.hpi.des.mpws2019.engine.stream.AStream;
import de.hpi.des.mpws2019.engine.window.Time;
import de.hpi.des.mpws2019.engine.window.assigner.GlobalWindow;
import de.hpi.des.mpws2019.engine.window.assigner.TumblingProcessingTimeWindow;
import de.hpi.des.mpws2019.engine.window.assigner.TumblingWindow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class EngineTest {

  @Test
  void testTumblingProcessingWindowJoin() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final ListSource<Integer> source = new ListSource<>(list);
    final ListSource<String> stringSource = new ListSource<>(
        List.of("2", "5", "4", "3"));

    final List<String> result = new LinkedList<>();
    final ListSink<String> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.window(TumblingWindow.ofProcessingTime(Time.seconds(5)))
        .join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    var engine = new Engine(builder);
    engine.run();
    while (!source.isDone() || !stringSource.isDone() || !(result.size() == 3)) {
      sleep(100);
    }
    assertThat(result).containsExactlyInAnyOrder("22", "44", "33");
  }

  @Test
  void testGlobalWindowJoin() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final ListSource<Integer> source = new ListSource<>(list);
    final ListSource<String> stringSource = new ListSource<>(
        List.of("2", "5", "4", "3"));

    final List<String> result = new LinkedList<>();
    final ListSink<String> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.window(new GlobalWindow())
        .join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    var engine = new Engine(builder);
    engine.run();
    while (!source.isDone() || !stringSource.isDone() || !(result.size() == 3)) {
      sleep(100);
    }
    assertThat(result).containsExactlyInAnyOrder("22", "44", "33");
  }

  @Test
  void testLongQuery() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }
    final var correctResult = List.copyOf(list).stream().map(i -> i + 1).filter(i -> i > 0)
        .map(i -> List.of(i, i, i)).flatMap(Collection::stream).map(i -> i * 3).filter(i -> i > 10)
        .collect(Collectors.toList());

    final var sourceInt = new ListSource<>(list);
    final LinkedList<Integer> results = new LinkedList<>();
    final var sink = new ListSink<>(results);
    final var builder = new TopologyBuilder();

    builder.streamOf(sourceInt).map(i -> i + 1).filter(i -> i > 0).map(i -> List.of(i, i, i))
        .flatMap(i -> i).map(i -> i * 3).filter(i -> i > 10).to(sink);

    var engine = new Engine(builder);
    engine.run();
    while (!sourceInt.isDone() || !(results.size() == correctResult.size())) {
      sleep(100);
    }

    assertThat(results).containsExactlyElementsOf(correctResult);


  }

}