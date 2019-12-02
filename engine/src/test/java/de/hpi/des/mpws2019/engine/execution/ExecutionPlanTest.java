package de.hpi.des.mpws2019.engine.execution;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.execution.plan.ExecutionPlan;
import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.graph.Topology;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.io.ListSink;
import de.hpi.des.mpws2019.engine.io.ListSource;
import de.hpi.des.mpws2019.engine.operation.Sink;
import de.hpi.des.mpws2019.engine.operation.Source;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExecutionPlanTest {

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

    stream.join(stringString, (i, s) -> s + i, (i, s) -> String.valueOf(i).equals(s)).to(sink);

    final Topology topology = builder.build();

    final ExecutionPlan plan = ExecutionPlan.from(topology);
    plan.getSlots().forEach(Slot::runStep);
    assertThat(result).containsExactly("22");
    plan.getSlots().forEach(Slot::runStep);
    assertThat(result).containsExactly("22");
    plan.getSlots().forEach(Slot::runStep);
    assertThat(result).containsExactly("22", "44");
    plan.getSlots().forEach(Slot::runStep);
    assertThat(result).containsExactly("22", "44", "33");
  }

}