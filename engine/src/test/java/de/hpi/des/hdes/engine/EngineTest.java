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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
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
    Query Q1 = new Query(builder.build());

    var engine = new Engine();
    engine.addQuery(Q1);
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
    Query Q1 = new Query(builder.build());

    var engine = new Engine();
    engine.addQuery(Q1);
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
    Query Q1 = new Query(builder.build());

    var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    while (!sourceInt.isDone() || !(results.size() == correctResult.size())) {
      sleep(100);
    }

    assertThat(results).containsExactlyElementsOf(correctResult);
  }

  @Test
  @Timeout(5)
  void testAddingSecondQuery() throws InterruptedException {
    final Integer sourceSize = 1000;
    final List<Integer> sourceList = IntStream.rangeClosed(1, sourceSize).boxed()
            .collect(Collectors.toList());
    final List<Integer> resultList = IntStream.rangeClosed(1, sourceSize).boxed()
            .map(i -> i + 1)
            .collect(Collectors.toList());

    final ListSource<Integer> sourceIntQ1 = new ListSource(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceIntQ1)
            .map(i -> i + 1)
            .to(sinkQ1);
    Query Q1 = new Query(builderQ1.build());

    var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    Thread.sleep(100);

    final ListSource<Integer> sourceIntQ2 = new ListSource(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceIntQ2)
            .map(i -> i + 1)
            .to(sinkQ2);
    Query Q2 = new Query(builderQ2.build());

    engine.addQuery(Q2);
    while (!(resultsQ1.size() == sourceSize) || !(resultsQ2.size() == sourceSize)) {
      sleep(20);
      log.trace("Results Q1: "+resultsQ1.size());
      log.trace("Results Q2: "+resultsQ2.size());
    }

    assertThat(resultsQ1).containsExactlyElementsOf(resultList);
    assertThat(resultsQ2).containsExactlyElementsOf(resultList);
  }

  @Test
  @Timeout(5)
  void testAddingSecondQueryToSameSource() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 30000; i++) {
      list.add(i);
    }

    final var correctResult = List.copyOf(list).stream()
            .map(i -> i + 1)
            .collect(Collectors.toList());

    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceQ1)
            .map(i -> i + 1)
            .filter(i -> i > 0)
            .to(sinkQ1);
    Query Q1 = new Query(builderQ1.build());

    var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    Thread.sleep(1);

    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1)
            .map(i -> i + 1)
            .map(i -> i * 2)
            .to(sinkQ2);
    Query Q2 = new Query(builderQ2.build());

    final var minDiff = resultsQ1.size();
    engine.addQuery(Q2);
    while (!(resultsQ1.size() == correctResult.size())) {
      log.trace("Is Done: " + sourceQ1.isDone());
      log.trace("Results Q1: " + resultsQ1.size());
      log.trace("Results Q2: " + resultsQ2.size());
      sleep(20);
    }

    assertThat(resultsQ1.size()).isEqualTo(correctResult.size());
    assertThat(resultsQ1.size() - resultsQ2.size()).isGreaterThanOrEqualTo(minDiff);
    assertThat( resultsQ2.size() - resultsQ1.size()).isLessThanOrEqualTo(0);
    log.debug("Results Q1: " + resultsQ1.size());
    log.debug("Results Q2: " + resultsQ2.size());
  }

  @Test
  @Timeout(5)
  void testDeleteStandaloneQuery() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 300000; i++) {
      list.add(i);
    }

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1)
            .map(i -> i + 1)
            .to(sinkQ1);
    Query Q1 = new Query(builderQ1.build());

    // Query 2
    final var sourceQ2 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ2)
            .map(i -> i + 3)
            .to(sinkQ2);
    Query Q2 = new Query(builderQ2.build());

    var engine = new Engine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);
    engine.run();
    Thread.sleep(5);
    engine.deleteQuery(Q2);

    ArrayList<Integer> sizeOfQ2Sink = new ArrayList<>();
    while (!(resultsQ1.size() == list.size())) {
      log.info("Is Done: " + sourceQ1.isDone());
      log.info("Results Q1: " + resultsQ1.size());
      log.info("Results Q2: " + resultsQ2.size());
      sizeOfQ2Sink.add(resultsQ2.size());
      sleep(5);
    }
    HashSet<Integer> sizeOfQ2SinkSet = new HashSet<>(sizeOfQ2Sink);
    // Make sure that the Sink of Q2 didn't change after shutting it down, or at least not more than twice (flaky CI)
    assertThat(sizeOfQ2SinkSet.size()).isLessThanOrEqualTo(2);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1.size()).isEqualTo(list.size());
    log.info("Results Q1: " + resultsQ1.size());
    log.info("Results Q2: " + resultsQ2.size());
  }

  @Test
  @Timeout(5)
  void testDeleteSharedSourceQuery() throws InterruptedException {
    final var list = new ArrayList<Integer>();
    for (int i = 0; i < 300000; i++) {
      list.add(i);
    }

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1)
            .map(i -> i + 1)
            .to(sinkQ1);
    Query Q1 = new Query(builderQ1.build());

    // Query 2
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1)
            .map(i -> i + 3)
            .to(sinkQ2);
    Query Q2 = new Query(builderQ2.build());

    var engine = new Engine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);
    engine.run();
    Thread.sleep(5);
    engine.deleteQuery(Q2);

    ArrayList<Integer> sizeOfQ2Sink = new ArrayList<>();
    while (!(resultsQ1.size() == list.size())) {
      log.info("Is Done: " + sourceQ1.isDone());
      log.info("Results Q1: " + resultsQ1.size());
      log.info("Results Q2: " + resultsQ2.size());
      sizeOfQ2Sink.add(resultsQ2.size());
      sleep(5);
    }
    HashSet<Integer> sizeOfQ2SinkSet = new HashSet<>(sizeOfQ2Sink);
    // Make sure that the Sink of Q2 didn't change after shutting it down, or at least not more than twice (flaky CI)
    assertThat(sizeOfQ2SinkSet.size()).isLessThanOrEqualTo(2);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1.size()).isEqualTo(list.size());
    log.info("Results Q1: " + resultsQ1.size());
    log.info("Results Q2: " + resultsQ2.size());
  }

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}