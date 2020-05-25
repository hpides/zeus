package de.hpi.des.hdes.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.execution.slot.VulcanoRunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.graph.vulcano.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.ArrayList;
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

  public static final int ENGINE_TIMEOUT = 30;

  @Test
  void testLongQuery() {
    final long eventCount = 10000;
    final int keyCount = 1000;

    final List<Integer> listS1 = new ArrayList<>();
    final List<Integer> listS2 = new ArrayList<>();

    for (int i = 0; i < eventCount; i++) {
      final int value = i % keyCount;
      listS1.add(value);
      listS2.add(value);
    }

    // for flushing windows of aggregation i.e. they are joined
    listS1.add(1_000_000_000);
    listS2.add(1_000_000_000);

    // for flushing windows of join
    listS1.add(1_000_000_001 + keyCount);
    listS2.add(1_000_000_002 + keyCount);

    final ListSource<Integer> sourceS1 = new ListSource<>(listS1, new WatermarkGenerator<>(keyCount - 1, 1), e -> e);
    final ListSource<Integer> sourceS2 = new ListSource<>(listS2, new WatermarkGenerator<>(keyCount - 1, 1), e -> e);

    final List<Integer> results = new LinkedList<>();
    final var sink = new ListSink<>(results);
    final var builder = new TopologyBuilder();

    final AStream<Integer> stream1 = builder.streamOf(sourceS1).map(i -> i + 1).map(i -> i - 1);
    final AStream<Integer> stream2 = builder.streamOf(sourceS2).map(i -> i + 1).map(i -> i - 1);

    stream1.window(TumblingWindow.ofEventTime(1))
        .join(stream2, (i, j) -> i + j, i -> i, i -> i, new WatermarkGenerator<>(keyCount, 1), e -> e / 2)
        .window(TumblingWindow.ofEventTime(1)).groupBy(e -> e).aggregate(new Aggregator<Integer, Integer, Integer>() {
          @Override
          public Integer initialize() {
            return 0;
          }

          @Override
          public Integer add(final Integer state, final Integer input) {
            return state + 1;
          }

          @Override
          public Integer getResult(final Integer result) {
            return result;
          }
        }, new WatermarkGenerator<>(-1, -1), e -> e).to(sink);

    final Query query = new Query(builder.build());

    final var engine = new VulcanoEngine();
    engine.addQuery(query);

    // eventCount + 2 because of the two watermarks
    for (int i = 0; i < eventCount + 2; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    for (int i = 0; i < keyCount; i++) {
      assertThat(results.get(i)).isEqualTo((int) Math.pow(eventCount / keyCount, 2));
    }
  }

  @Test
  void testAddingSecondQuery() {
    final int sourceSize = 1000;
    final List<Integer> sourceList = IntStream.rangeClosed(1, sourceSize).boxed().collect(Collectors.toList());
    final List<Integer> resultList = IntStream.rangeClosed(1, sourceSize).boxed().map(i -> i + 1)
        .collect(Collectors.toList());

    final ListSource<Integer> sourceIntQ1 = new ListSource(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceIntQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);

    for (int i = 0; i < sourceSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    final ListSource<Integer> sourceIntQ2 = new ListSource<>(List.copyOf(sourceList));
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceIntQ2).map(i -> i + 1).to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());
    engine.addQuery(Q2);

    for (int i = 0; i < sourceSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    assertThat(resultsQ1).containsExactlyElementsOf(resultList);
    assertThat(resultsQ2).containsExactlyElementsOf(resultList);
  }

  @Test
  void testAddingSecondQueryToSameSource() {
    final int sourceSize = 50_000;
    final int breakSize = sourceSize / 2;
    final List<Integer> list = IntStream.range(0, sourceSize).boxed().collect(Collectors.toList());

    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();

    builderQ1.streamOf(sourceQ1).map(i -> i + 1).filter(i -> i > 0).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);

    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1).map(i -> i + 1).map(i -> i * 2).to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());

    engine.addQuery(Q2);
    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    assertThat(resultsQ1).hasSize(sourceSize);
    assertThat(resultsQ2).hasSize(breakSize);
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testSlotFinishedAfterDeletion() throws InterruptedException {
    final List<Integer> list = IntStream.range(0, 10_000).boxed().collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());
    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);
    engine.run();
    sleep(20);
    engine.deleteQuery(Q1);
    sleep(20);
    final List<Slot<?>> runnableSlots = engine.getPlan().getSlots();
    assertThat(runnableSlots).allMatch(Slot::isShutdown);
  }

  @Test
  @Timeout(ENGINE_TIMEOUT)
  void testSlotShutdownAfterDeletion() throws InterruptedException {
    final int sourceSize = 10_000;
    final List<Integer> list = IntStream.range(0, sourceSize).boxed().collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());
    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);
    engine.run();
    sleep(20);
    engine.deleteQuery(Q1);
    sleep(20);
    final List<VulcanoRunnableSlot<?>> runnableSlots = engine.getPlan().getRunnableSlots();
    assertThat(runnableSlots).allMatch(VulcanoRunnableSlot::isShutdown);
  }

  @Test
  void testDeleteStandaloneQuery() {
    final int sourceSize = 30_000;
    final int breakSize = sourceSize / 2;

    final List<Integer> list = IntStream.range(0, sourceSize).boxed().collect(Collectors.toList());

    // Query 1
    final var builderQ1 = TopologyBuilder.newQuery();
    final var sourceQ1 = new ListSource<>(list);

    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);

    builderQ1.streamOf(sourceQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = builderQ1.buildAsQuery();

    // Query 2
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var builderQ2 = TopologyBuilder.newQuery();
    final var sourceQ2 = new ListSource<>(List.copyOf(list));

    final var sinkQ2 = new ListSink<>(resultsQ2);

    builderQ2.streamOf(sourceQ2).map(i -> i + 3).to(sinkQ2);
    final Query Q2 = builderQ2.buildAsQuery();

    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);

    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }
    engine.deleteQuery(Q2);
    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }
    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1).hasSize(sourceSize);
    assertThat(resultsQ2).hasSize(breakSize);
  }

  @Test
  void testDeleteSharedSourceQuery() {
    final int sourceSize = 30_000;
    final int breakSize = sourceSize / 2;
    final List<Integer> list = IntStream.range(0, sourceSize).boxed().collect(Collectors.toList());

    // Query 1
    final var builderQ1 = new TopologyBuilder();
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    builderQ1.streamOf(sourceQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    // Query 2
    final LinkedList<Integer> resultsQ2 = new LinkedList<>();
    final var sinkQ2 = new ListSink<>(resultsQ2);
    final var builderQ2 = new TopologyBuilder();
    builderQ2.streamOf(sourceQ1).map(i -> i + 3).to(sinkQ2);
    final Query Q2 = new Query(builderQ2.build());

    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);
    engine.addQuery(Q2);

    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }
    engine.deleteQuery(Q2);
    for (int i = 0; i < breakSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }
    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(resultsQ1.size()).isGreaterThan(resultsQ2.size());
    assertThat(resultsQ1).hasSize(sourceSize);
    assertThat(resultsQ2).hasSize(breakSize);
  }

  @Test
  void testSimpleQuery() {
    final int sourceSize = 30_000;
    final List<Integer> list = IntStream.range(0, sourceSize).boxed().collect(Collectors.toList());

    // Query 1
    final var sourceQ1 = new ListSource<>(list);
    final LinkedList<Integer> resultsQ1 = new LinkedList<>();
    final var sinkQ1 = new ListSink<>(resultsQ1);
    final var builderQ1 = new TopologyBuilder();
    builderQ1.streamOf(sourceQ1).map(i -> i + 1).to(sinkQ1);
    final Query Q1 = new Query(builderQ1.build());

    final var engine = new VulcanoEngine();
    engine.addQuery(Q1);

    for (int i = 0; i < sourceSize; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    final List<Integer> expected = list.stream().map(i -> i + 1).collect(Collectors.toList());
    assertThat(resultsQ1.size()).isEqualTo(sourceSize);
    assertThat(resultsQ1).containsExactlyInAnyOrderElementsOf(expected);
  }

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}