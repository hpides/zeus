package de.hpi.des.hdes.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.TimeWindow;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingEventTimeWindow;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
class EngineTest {

  public static final int ENGINE_TIMEOUT = 30;

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testLongQuery() throws InterruptedException {
    final long eventCount = 10000;
    final int keyCount = 1000;

    final List<Integer> listS1 = new ArrayList<Integer>();
    final List<Integer> listS2 = new ArrayList<Integer>();

    for (int i = 0; i < eventCount; i++) {
      int value = i % keyCount;
      listS1.add(value);
      listS2.add(value);
    }


    // for flushing windows of aggregation i.e. they are joined
    listS1.add(1_000_000_000);
    listS2.add(1_000_000_000);

    // for flushing windows of join
    listS1.add(1_000_000_001 + keyCount);
    listS2.add(1_000_000_002 + keyCount);

    final ListSource<Integer> sourceS1 =
      new ListSource<>(listS1, new WatermarkGenerator<>(keyCount - 1, 1), e -> e);
    final ListSource<Integer> sourceS2 =
      new ListSource<>(listS2, new WatermarkGenerator<>(keyCount - 1, 1), e -> e);

    final List<Integer> results = new LinkedList<>();
    final var sink = new ListSink<>(results);
    final var builder = new TopologyBuilder();

    AStream<Integer> stream1 = builder.streamOf(sourceS1).map(i -> i + 1).map(i -> i - 1);
    AStream<Integer> stream2 = builder.streamOf(sourceS2).map(i -> i + 1).map(i -> i - 1);

    stream1.window(TumblingWindow.ofEventTime(1))
      .join(
        stream2,
        (i, j) -> i + j,
        i -> i,
        i -> i,
        new WatermarkGenerator<>(keyCount, 1),
        e -> e/2)
      .window(TumblingEventTimeWindow.ofEventTime(1))
      .groupBy(e -> e)
      .aggregate(new Aggregator<Integer, Integer, Integer>() {
        @Override
        public Integer initialize() {
          return 0;
        }

        @Override
        public Integer add(Integer state, Integer input) {
          return state + 1;
        }

        @Override
        public Integer getResult(Integer result) {
          return result;
        }
      }, new WatermarkGenerator<>(-1, -1), e -> e)
      .to(sink);

    final Query query = new Query(builder.build());

    final var engine = new Engine();
    engine.addQuery(query);
    engine.run();
//    long totalResultSize = (long) Math.pow(eventCount/keyCount, 2)*keyCount; // + 1 for flush event
    long totalResultSize = keyCount;
    log.debug("Total results: {}", totalResultSize);
    while (!sourceS1.isDone() || results.size() != totalResultSize) {
      log.debug("Results: {}", results.size());
      log.debug("Results: {}", results);
      sleep(100);
    }

    for (int i = 0; i < keyCount ; i++) {
      assertThat(results.get(i)).isEqualTo((int) Math.pow(eventCount/keyCount, 2));
    }
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testAddingSecondQuery() throws InterruptedException {
    final Integer sourceSize = 1000;
    final List<Integer> sourceList = IntStream.rangeClosed(1, sourceSize).boxed()
        .collect(Collectors.toList());
    final List<Integer> resultList = IntStream.rangeClosed(1, sourceSize).boxed()
        .map(i -> i + 1).collect(Collectors.toList());

    final ListSource<Integer> sourceIntQ1 = new ListSource(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceIntQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    sleep(100);

    final ListSource<Integer> sourceIntQ2 = new ListSource<>(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceIntQ2)
        .map(i -> i + 1)
        .to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());

    engine.addQuery(Q2);
    while (resultsQ1.size() != sourceSize || resultsQ2.size() != sourceSize) {
      sleep(20);
      log.trace("Results Q1: " + resultsQ1.size());
      log.trace("Results Q2: " + resultsQ2.size());
    }

    assertThat(resultsQ1).containsExactlyElementsOf(resultList);
    assertThat(resultsQ2).containsExactlyElementsOf(resultList);
  }

  @Test
  //@Timeout(ENGINE_TIMEOUT)
  void testAddingSecondQueryToSameSource() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 50_000).boxed()
        .collect(Collectors.toList());

    final var correctResult = list.stream()
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
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    sleep(10);
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1)
        .map(i -> i + 1)
        .map(i -> i * 2)
        .to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());

    final var minDiff = resultsQ1.size();
    engine.addQuery(Q2);
    while (resultsQ1.size() != correctResult.size()) {
      log.debug("Is Done: {}", sourceQ1.isDone());
      log.debug("Results Q1: {}", resultsQ1.size());
      log.debug("Results Q2: {}", resultsQ2.size());
      sleep(200);
    }


    assertThat(resultsQ1.size()).isEqualTo(correctResult.size());
    assertThat(resultsQ1.size() - resultsQ2.size()).isGreaterThanOrEqualTo(minDiff);
    assertThat(resultsQ2.size() - resultsQ1.size()).isLessThanOrEqualTo(0);
    log.debug("Results Q1: {}", resultsQ1.size());
    log.debug("Results Q2: {}", resultsQ2.size());
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testSlotFinishedAfterDeletion() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 10_000).boxed()
        .collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());
    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    Thread.sleep(20);
    engine.deleteQuery(Q1);
    Thread.sleep(20);
    final List<Slot<?>> runnableSlots = engine.getPlan().getSlots();
    assertThat(runnableSlots).allMatch(Slot::isShutdown);
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testSlotShutdownAfterDeletion() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 10_000).boxed()
        .collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());
    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    Thread.sleep(20);
    engine.deleteQuery(Q1);
    Thread.sleep(20);
    final List<RunnableSlot<?>> runnableSlots = engine.getPlan().getRunnableSlots();
    assertThat(runnableSlots).allMatch(RunnableSlot::isShutdown);
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testDeleteStandaloneQuery() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 30_000).boxed()
        .collect(Collectors.toList());

    // Query 1
    final var builderQ1 = TopologyBuilder.newQuery();
    final var sourceQ1 = new ListSource<>(list);

    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);

    builderQ1.streamOf(sourceQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = builderQ1.buildAsQuery();

    // Query 2
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var builderQ2 = TopologyBuilder.newQuery();
    final var sourceQ2 = new ListSource<>(List.copyOf(list));

    final var sinkQ2 = new ListSink<>(resultsQ2);

    builderQ2.streamOf(sourceQ2)
        .map(i -> i + 3)
        .to(sinkQ2);
    final Query Q2 = builderQ2.buildAsQuery();

    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);
    engine.run();
    sleep(5);
    engine.deleteQuery(Q2);
    sleep(10);
    final Set<Integer> uniqueQ2SinkSizes = new HashSet<>();
    while (resultsQ1.size() < list.size()) {
      log.info("Is Done: {}", sourceQ1.isDone());
      log.info("Results Q1: {}", resultsQ1.size());
      log.info("Results Q2: {}", resultsQ2.size());
      uniqueQ2SinkSizes.add(resultsQ2.size());
      sleep(200);
    }
    engine.shutdown();
    // Make sure the Sink of Q2 didn't change after shutting it down
    log.info("Results Q1: {}", resultsQ1.size());
    log.info("Results Q2: {}", resultsQ2.size());
    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(uniqueQ2SinkSizes.size()).isLessThanOrEqualTo(1);
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1.size()).isEqualTo(list.size());
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testDeleteSharedSourceQuery() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 30_000).boxed()
        .collect(Collectors.toList());

    // Query 1
    final var builderQ1 = new TopologyBuilder();
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    builderQ1.streamOf(sourceQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    // Query 2
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1)
        .map(i -> i + 3)
        .to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());

    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);
    engine.run();
    sleep(5);
    engine.deleteQuery(Q2);
    // some time to delete the query and make the test stable
    sleep(50);
    final Set<Integer> uniqueQ2SinkSizes = new HashSet<>();
    while (resultsQ1.size() < list.size()) {
      log.info("Is Done: {}", sourceQ1.isDone());
      log.info("Results Q1: {}", resultsQ1.size());
      log.info("Results Q2: {}", resultsQ2.size());
      uniqueQ2SinkSizes.add(resultsQ2.size());
      sleep(200);
    }
    engine.shutdown();
    // Make sure the Sink of Q2 didn't change after shutting it down
    log.info("Results Q1: {}", resultsQ1.size());
    log.info("Results Q2: {}", resultsQ2.size());
    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(uniqueQ2SinkSizes.size()).isLessThanOrEqualTo(1);
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1.size()).isEqualTo(list.size());
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testSimpleQuery() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 30_000).boxed()
        .collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1)
        .map(i -> i + 1)
        .to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new Engine();
    engine.addQuery(Q1);
    engine.run();
    sleep(5);

    while (resultsQ1.size() < list.size()) {
      log.info("Is Done: {}", sourceQ1.isDone());
      log.info("Results Q1: {}", resultsQ1.size());
      sleep(200);
    }

    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(resultsQ1.size()).isEqualTo(expected.size());
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
    log.info("Results Q1: {}", resultsQ1.size());
  }

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}