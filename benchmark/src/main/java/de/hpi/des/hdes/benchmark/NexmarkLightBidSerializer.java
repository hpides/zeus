package de.hpi.des.hdes.benchmark;


import org.jooq.lambda.tuple.Tuple5;

public class NexmarkLightBidSerializer extends AbstractSerializer<Tuple5<Long, Long, Integer, Integer, Long>> {

  @Override
  public String serialize(Tuple5<Long, Long, Integer, Integer, Long> obj) {
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
  public Tuple5<Long, Long, Integer, Integer, Long> deserialize(String obj) {
    return null;
  }
}
