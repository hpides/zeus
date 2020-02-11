package de.hpi.des.hdes.engine.shared.join;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.Engine;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.assigner.TumblingEventTimeWindow;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class AJoinTest {

  @Test
  @Timeout(5)
  void testAJoin() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final ListSource<Integer> source = new ListSource<>(list);
    final ListSource<Integer> source2 = new ListSource<>(
        List.of(1, 2, 3));

    final Set<Integer> result = new HashSet<>();
    final Sink<Integer> sink = i -> result.add(i.getValue());

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source);
    final AStream<Integer> stream2 = builder.streamOf(source2);

    stream.window(new TumblingEventTimeWindow(Time.of(1000).getNanos()))
        .ajoin(stream2,
            i -> i,
            i -> i,
            Integer::sum)
        .to(sink);

    ExecutionConfig.makeShortTimoutConfig();

    final var engine = new Engine();
    final Query query = new Query(builder.build());
    engine.addQuery(query);
    engine.run();
    while (result.size() != 3) {
      sleep(200);
    }
    assertThat(result).contains(2, 4, 6);
  }

  @Test
  @Timeout(5)
  void testQuerySharing() throws InterruptedException {
    final List<Integer> listSource1 = List.of(4, 5, 6);
    final List<Integer> listSource2 = List.of(4, 5, 6);

    final ListSource<Integer> source = new ListSource<>(listSource1);
    final ListSource<Integer> source2 = new ListSource<>(listSource2);

    // build query 1
    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source);
    final AStream<Integer> stream2 = builder.streamOf(source2);

    final Set<Integer> resultSet = new HashSet<>();
    final Sink<Integer> sink = i -> resultSet.add(i.getValue());

    stream.window(new TumblingEventTimeWindow(Time.seconds(1).getNanos()))
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

    query2Stream1.window(new TumblingEventTimeWindow(Time.seconds(1).getNanos()))
        .ajoin(query2Stream2,
            i -> i,
            i -> i,
            Integer::max)
        .to(query2Sink);

    ExecutionConfig.makeShortTimoutConfig();

    final Query query = new Query(builder.build());
    final Query query2 = new Query(query2Builder.build());

    final Engine engine = new Engine();
    engine.addQuery(query);
    engine.addQuery(query2);
    engine.run();
    while (resultSet.size() != 3) {
      sleep(200);
    }

    assertThat(resultSet).containsExactly(8, 10, 12);
    assertThat(query2ResultSet).containsExactly(4, 5, 6);
  }
}
