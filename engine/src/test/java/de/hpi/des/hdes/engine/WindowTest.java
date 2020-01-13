package de.hpi.des.hdes.engine;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.aggregators.SumAggregator;
import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class WindowTest extends ShortTimoutSetup {

  @Test
  void testTumblingProcessingTimeWindow() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final Source<Integer> source = new ListSource<>(list);
    final Source<String> stringSource = new ListSource<>(
        List.of("2", "5", "4", "3"));

    final List<String> result = new LinkedList<>();
    final Sink<String> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.window(TumblingWindow.ofProcessingTime(Time.seconds(1)))
        .join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    final Query query = new Query(builder.build());
    var slots = ExecutionPlan.from(query).getSlots();

    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44");
    Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // Waiting for the window to close
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44");
  }

  @Test
  void testAggregation() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3, 4);
    final Source<Integer> source = new ListSource<>(list);

    final List<Integer> result = new LinkedList<>();
    final Sink<Integer> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);

    stream.window(TumblingWindow.ofProcessingTime(Time.seconds(1)))
            .aggregate(new SumAggregator()).to(sink);

    final Query query = new Query(builder.build());

    var slots = ExecutionPlan.from(query).getSlots();

    log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
    log.debug("Nanotime: " + System.nanoTime());
    TestUtil.runAndTick(slots);
    log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
    log.debug("Nanotime: " + System.nanoTime());
    TestUtil.runAndTick(slots);
    log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
    log.debug("Nanotime: " + System.nanoTime());
    TestUtil.runAndTick(slots);
    log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
    log.debug("Nanotime: " + System.nanoTime());
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    TestUtil.runAndTick(slots);
    log.debug("Result: " + result.stream().map(i -> toString()).collect(Collectors.joining(", ")));
    log.debug("Nanotime: " + System.nanoTime()/1000);
    assertThat(result).containsExactly(9);
  }

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}
