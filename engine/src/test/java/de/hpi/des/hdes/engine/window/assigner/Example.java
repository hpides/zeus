package de.hpi.des.hdes.engine.window.assigner;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.aggregators.SumAggregator;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

public class Example {

  @NoArgsConstructor
  @Data
  class Session {
    private long duration;
    private String userId;
    private String objectId;
  }

  @NoArgsConstructor
  @Data
  class Purchase {
    private long timestamp;
    private String userId;
    private String objectId;
  }

  int MIN_DURATION = 50;

  @Test
  void buildExample() {
    ListSource<Session> sessionSource = new ListSource<>(List.of(new Session()));
    ListSource<Purchase> purchaseSource = new ListSource<>(List.of(new Purchase()));
    ListSink<Integer> listSink = new ListSink<>(new ArrayList<>());

    VulcanoTopologyBuilder builder = VulcanoTopologyBuilder.newQuery();
    AStream<Purchase> purchaseStream = builder.streamOf(purchaseSource);
    builder.streamOf(sessionSource).filter(session -> session.getDuration() > MIN_DURATION)
        .window(TumblingWindow.ofEventTime(Time.seconds(20)))
        .join(purchaseStream, (view, purchase) -> purchase, Session::getUserId, Purchase::getUserId,
            new WatermarkGenerator<>(0, 0), Purchase::getTimestamp)
        .window(TumblingWindow.ofEventTime(Time.seconds(20))).groupBy(i -> i)
        .aggregate(new CountAggregator<>(), new WatermarkGenerator<>(0, 0), i -> i);
    Query query = builder.buildAsQuery();
  }

  public class CountAggregator<K> implements Aggregator<K, Integer, Integer> {

    @Override
    public Integer initialize() {
      return 0;
    }

    @Override
    public Integer add(final Integer state, final K input) {
      return state + 1;
    }

    @Override
    public Integer getResult(final Integer state) {
      return state;
    }
  }

}
