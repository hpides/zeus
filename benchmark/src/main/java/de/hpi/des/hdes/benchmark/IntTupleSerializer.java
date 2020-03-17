package de.hpi.des.hdes.benchmark;


import java.util.StringTokenizer;
import org.jooq.lambda.tuple.Tuple2;

public class IntTupleSerializer extends AbstractSerializer<Tuple2<Integer, Long>> {

  @Override
  public String serialize(Tuple2<Integer, Long> obj) {
    return obj.v1.toString().concat(",").concat(obj.v2.toString());
  }

  @Override
  public Tuple2<Integer, Long> deserialize(String obj) {
    var vals = new StringTokenizer(obj, ",");
    return new Tuple2<>(Integer.parseInt(vals.nextToken()), Long.parseLong(vals.nextToken()));

  }
}
