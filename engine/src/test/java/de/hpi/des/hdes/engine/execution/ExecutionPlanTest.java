package de.hpi.des.hdes.engine.execution;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.TestUtil;
import de.hpi.des.hdes.engine.execution.ExecutionConfig.ShortTimeoutConfig;
import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExecutionPlanTest extends ShortTimeoutConfig {

  @Test
  void shouldCreateExecutablePlanFromTopology() throws InterruptedException {
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

    final Topology topology = builder.build();

    var slots = ExecutionPlan.from(topology).getSlots();

    TestUtil.stepSleepAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.stepSleepAndTick(slots);
    assertThat(result).containsExactly("22");
    TestUtil.stepSleepAndTick(slots);
    assertThat(result).containsExactly("22", "44");
    TestUtil.stepSleepAndTick(slots);
    assertThat(result).containsExactly("22", "44", "33");
  }


}