package de.hpi.des.hdes.engine.shared.join;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.Engine;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.TestUtil;
import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class AJoinTest {

  @Test
  void testAJoin() {
    final List<TestValue> list = Seq.of(1).cycle(3).map(TestValue::newValue).toList();
    final List<TestValue> list2 = Seq.of(1).cycle(3).map(TestValue::newValue).toList();

    list.add(TestValue.of(-1, 5_000));
    list2.add(TestValue.of(-10, 5_000));

    final int size = list.size();
    final WatermarkGenerator<TestValue> generator = new WatermarkGenerator<>(1, size);
    final WatermarkGenerator<TestValue> generator1 = new WatermarkGenerator<>(1, size);
    final Source<TestValue> source = new ListSource<>(list, generator, TestValue::getTimestamp);
    final Source<TestValue> source1 = new ListSource<>(list2, generator1, TestValue::getTimestamp);

    final List<TestResult> result = new ArrayList<>();
    final Sink<TestResult> sink = i -> result.add(i.getValue());

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<TestValue> stream = builder.streamOf(source);
    final AStream<TestValue> stream1 = builder.streamOf(source1);

    stream.window(TumblingWindow.ofEventTime(300))
        .ajoin(stream1,
            TestValue::getId,
            TestValue::getId,
            TestResult::of)
        .to(sink);

    final Engine engine = new Engine();
    final Query query = new Query(builder.build());
    engine.addQuery(query);

    for (int i = 0; i < size * 2; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    final List<TestResult> expected = Seq.seq(list)
        .innerJoin(list2, (a, b) -> a.getId() == b.getId())
        .map(t -> TestResult.of(t.v1, t.v2))
        .toList();

    assertThat(result).containsExactlyInAnyOrderElementsOf(expected);

  }

  @Test
  void testQuerySharing() {
    final List<Integer> listSource1 = List.of(4, 5, 6, 100);
    final List<Integer> listSource2 = List.of(4, 5, 6, 1001);

    final WatermarkGenerator<Integer> generator = new WatermarkGenerator<>(1, 4);
    final WatermarkGenerator<Integer> generator1 = new WatermarkGenerator<>(1, 4);

    final ListSource<Integer> source = new ListSource<>(listSource1, generator, e -> e * 100);
    final ListSource<Integer> source2 = new ListSource<>(listSource2, generator1, e -> e * 100);

    // build query 1
    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source);
    final AStream<Integer> stream2 = builder.streamOf(source2);

    final Set<Integer> resultSet = new HashSet<>();
    final Sink<Integer> sink = i -> resultSet.add(i.getValue());

    stream.window(TumblingWindow.ofEventTime(100))
        .ajoin(stream2,
            i -> i,
            i -> i,
            Integer::sum)
        .to(sink);

    // build query 2
    final TopologyBuilder query2Builder = new TopologyBuilder();
    final AStream<Integer> query2Stream1 = query2Builder.streamOf(source);
    final AStream<Integer> query2Stream2 = query2Builder.streamOf(source2);

    final Set<Integer> query2ResultSet = new HashSet<>();
    final Sink<Integer> query2Sink = i -> query2ResultSet.add(i.getValue());

    query2Stream1.window(TumblingWindow.ofEventTime(100))
        .ajoin(query2Stream2,
            i -> i,
            i -> i,
            Integer::max)
        .to(query2Sink);

    final Query query = new Query(builder.build());
    final Query query2 = new Query(query2Builder.build());

    final Engine engine = new Engine();
    engine.addQuery(query);
    engine.addQuery(query2);

    for (int i = 0; i < 6; i++) {
      TestUtil.runAndTick(engine.getPlan().getRunnableSlots());
    }

    assertThat(resultSet).containsExactly(8, 10, 12);
    assertThat(query2ResultSet).containsExactly(4, 5, 6);
  }

  @BeforeAll
  static void setUp() {
    ExecutionConfig.makeShortTimoutConfig();
  }

  private static Random random = new Random();

  @Value(staticConstructor = "of")
  @EqualsAndHashCode
  private static final class TestValue {

    private int id;
    private int timestamp;

    static TestValue newValue(final int id) {
      final int timestamp = random.nextInt(200);
      return new TestValue(id, timestamp);
    }
  }

  @Value(staticConstructor = "of")
  private static final class TestResult {

    final TestValue leftTestValue;
    final TestValue rightTestValue;

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof TestResult)) {
        return false;
      }

      final TestResult other = (TestResult) obj;

      return (this.leftTestValue.equals(other.leftTestValue) &&
          this.rightTestValue.equals(other.rightTestValue)) ||
          (this.leftTestValue.equals(other.rightTestValue) &&
              this.rightTestValue.equals(other.leftTestValue));

    }
  }
}
