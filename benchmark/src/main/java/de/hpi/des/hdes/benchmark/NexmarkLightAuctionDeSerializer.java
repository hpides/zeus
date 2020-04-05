package de.hpi.des.hdes.benchmark;


import java.util.StringTokenizer;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple4;

public class NexmarkLightAuctionDeSerializer extends AbstractSerializer<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> {

  @Override
  public String serialize(Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long> obj) {
    return null;
  }

  @Override
  public Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long> deserialize(String obj) {
    var vals = new StringTokenizer(obj, ",");
    return new Tuple2<>(new Tuple4<>(Long.parseLong(vals.nextToken()),
        Integer.parseInt(vals.nextToken()),
        Integer.parseInt(vals.nextToken()),
        Integer.parseInt(vals.nextToken())), Long.parseLong(vals.nextToken()));
  }
}
