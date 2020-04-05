package de.hpi.des.hdes.benchmark;


import java.util.StringTokenizer;
import org.jooq.lambda.tuple.Tuple5;

public class NexmarkLightAuctionSerializer extends AbstractSerializer<Tuple5<Long, Integer, Integer, Integer, Long>> {

  @Override
  public String serialize(Tuple5<Long, Integer, Integer, Integer, Long> obj) {
    return obj.v1.toString().concat(",")
        .concat(obj.v2.toString())
        .concat(",")
        .concat(obj.v3.toString())
        .concat(",")
        .concat(obj.v4.toString())
        .concat(",")
        .concat(obj.v5.toString());
  }

  @Override
  public Tuple5<Long, Integer, Integer, Integer, Long> deserialize(String obj) {
    var vals = new StringTokenizer(obj, ",");
    return new Tuple5<>(Long.parseLong(vals.nextToken()),
        Integer.parseInt(vals.nextToken()),
        Integer.parseInt(vals.nextToken()),
        Integer.parseInt(vals.nextToken()),
        Long.parseLong(vals.nextToken()));
  }
}
