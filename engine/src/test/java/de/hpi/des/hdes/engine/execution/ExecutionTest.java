package de.hpi.des.hdes.engine.execution;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.TestUtil;
import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionTest {

  @Test
  void simpleExecutionTest() {
    final List<Integer> list = List.of(1, 2, 3);
    final Source<Integer> source = new ListSource<>(list);
    final TopologyBuilder builder = new TopologyBuilder();
    final List<Integer> result = new LinkedList<>();
    final Sink<Integer> sink = new ListSink(result);
    builder.streamOf(source).map(i -> i+1).to(sink);
    final Query query = new Query(builder.build());

    List<RunnableSlot<?>> runnableSlots = ExecutionPlan.emptyExecutionPlan().extend(query).getRunnableSlots();
    TestUtil.runAndTick(runnableSlots);
    TestUtil.runAndTick(runnableSlots);
    TestUtil.runAndTick(runnableSlots);
    assertThat(result).containsExactly(2, 3, 4);
  }

  @Test
  void simpleJoinTest() {
    final List<Integer> list = List.of(2, 3, 4);
    final Source<Integer> source = new ListSource<>(list);
    final Source<String> stringSource = new ListSource<>(
      List.of("2", "5", "4", "3"));

    final List<String> result = new LinkedList<>();
    final Sink<String> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.window(GlobalWindow.create())
      .join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    final Query query = new Query(builder.build());

    var slots = ExecutionPlan.emptyExecutionPlan().extend(query).getRunnableSlots();

    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44", "33");
  }

  @Test
  void shouldCreateExecutablePlanFromTopology() {
    final List<Integer> list = List.of(1, 2, 3);
    final Source<Integer> source = new ListSource<>(list);
    final Source<String> stringSource = new ListSource<>(
        List.of("2", "5", "4", "3"));

    final List<String> result = new LinkedList<>();
    final Sink<String> sink = new ListSink<>(result);

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
    final AStream<String> stringString = builder.streamOf(stringSource);

    stream.window(GlobalWindow.create())
        .join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    final Query query = new Query(builder.build());

    var slots = ExecutionPlan.emptyExecutionPlan().extend(query).getRunnableSlots();

    /**
     * Note for the future. This setup requires a topological order of the slots. O.w. it will fail.
     */
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44");
    TestUtil.runAndTick(slots);
    assertThat(result).containsExactly("22", "44", "33");
  }

  // TODO USE Nicos PR
  @BeforeAll
  static void shortTimeout() {
  	ExecutionConfig.makeShortTimoutConfig();
  }

}