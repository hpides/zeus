package de.hpi.des.hdes.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
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

  @Test
  void testAddingSecondQuery() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }

    final var correctResult = List.copyOf(list).stream()
            .map(i -> i + 1)
            .filter(i -> i > 0)
            .collect(Collectors.toList());

    final var sourceIntQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceIntQ1)
            .map(i -> i + 1)
            .to(sinkQ1);

    var engine = new Engine(builderQ1);
    engine.run();
    Thread.sleep(1000);

    final var sourceIntQ2 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceIntQ2)
            .map(i -> i + 1)
            .to(sinkQ2);

    engine.addQuery(builderQ2);
    while ((!sourceIntQ1.isDone() && !sourceIntQ2.isDone()) ||
            !(resultsQ1.size() == correctResult.size()) && !(resultsQ2.size() == correctResult.size())) {
      sleep(100);
      System.out.println("Results Q1: "+resultsQ1.size());
      System.out.println("Results Q2: "+resultsQ2.size());
    }

    assertThat(resultsQ1).containsExactlyElementsOf(correctResult);
    assertThat(resultsQ2).containsExactlyElementsOf(correctResult);
  }

  @Test
  void testAddingSecondQueryToSameSource() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 300000; i++) {
      list.add(i);
    }

    final var correctResult = List.copyOf(list).stream()
            .map(i -> i + 1)
            .collect(Collectors.toList());

    final var sourceIntQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceIntQ1)
            .map(i -> i + 1)
            .filter(i -> i > 0)
            .to(sinkQ1);

    var engine = new Engine(builderQ1);
    engine.run();
    Thread.sleep(10);

    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceIntQ1)
            .map(i -> i + 1)
            .map(i -> i * 2)
            .to(sinkQ2);

    final var minDiff = resultsQ1.size();
    engine.addQuery(builderQ2);
    while ((!sourceIntQ1.isDone()) || !(resultsQ1.size() == correctResult.size())) {
      System.out.println("Is Done: " + sourceIntQ1.isDone());
      System.out.println("Results Q1: " + resultsQ1.size());
      System.out.println("Results Q2: " + resultsQ2.size());
      sleep(100);
    }

    assertThat(resultsQ1.size()).isEqualTo(correctResult.size());
    assertThat(resultsQ1.size() - resultsQ2.size()).isGreaterThanOrEqualTo(minDiff);
    assertThat( resultsQ2.size() - resultsQ1.size()).isLessThanOrEqualTo(0);
    System.out.println("Results Q1: " + resultsQ1.size());
    System.out.println("Results Q2: " + resultsQ2.size());
  }

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}