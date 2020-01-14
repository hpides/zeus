package de.hpi.des.hdes.engine.ajoin;

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
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class AJoinTest {

  @Test
  @Timeout(60)
  void testAJoin() throws InterruptedException {
    final List<Integer> list = List.of(1, 2, 3);
    final ListSource<Integer> source = new ListSource<>(list);
    final ListSource<Integer> source2 = new ListSource<>(
        List.of(1, 2, 3));

    final Set<Integer> result = new HashSet<>();
    final Sink<Integer> sink = result::add;

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Integer> stream = builder.streamOf(source);
    final AStream<Integer> stream2 = builder.streamOf(source2);

    stream.window(TumblingWindow.ofProcessingTime(Time.of(500)))
        .ajoin(stream2,
            i -> i,
            i -> i,
            Integer::sum)
        .to(sink);

    ExecutionConfig.makeShortTimoutConfig();

    var engine = new Engine();
    Query query = new Query(builder.build());
    engine.addQuery(query);
    engine.run();
    while (result.size() != 3) {
      sleep(200);
    }
    assertThat(result).contains(2, 4, 6);
  }
}
