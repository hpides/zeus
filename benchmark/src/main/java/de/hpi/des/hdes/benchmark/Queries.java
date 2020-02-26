package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.udf.Mapper;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import java.util.function.Function;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;

public class Queries {

  private Queries() {
  }

  /**
   * Nooop query
   */
  public static <T> Query makeQuery0(Source<T> source, Sink<T> sink) {
    return new Query(
        new TopologyBuilder().streamOf(source).map(e -> e).to(sink).build());
  }

  public static <T> Query makeQuery0Measured(Source<T> source, Sink<Tuple> sink) {
    return
        new TopologyBuilder().streamOf(source).map(Queries::prepare).map(e -> e)
            .map(Queries::calcDelta).to(sink).buildAsQuery();
  }

  public static <In, Other> Join<Tuple2<In, Long>, Tuple2<Other, Long>, Tuple3<In, Other, Long>> makeJoinF() {
    return (Tuple2<In, Long> t1, Tuple2<Other, Long> t2) -> new Tuple3<>(t1.v1, t2.v1,
        Math.max(t1.v2, t2.v2));
  }

  public static <T1, T2> Query makePlainJoin0Measured(Source<T1> source1,
      Source<T2> source2, Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(source1).map(Queries::prepare);
    var s2 = tp.streamOf(source2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofProcessingTime(Time.seconds(5)))
        .join(s2, makeJoinF(), t1 -> t1.v1, t2 -> t2.v1, WatermarkGenerator.seconds(0, 1_000),
            TimestampExtractor.currentTimeNS());
    return j1.map(Queries::calcDelta).to(sink).buildAsQuery();
  }

  public static <T> Tuple2<T, Long> prepare(T value) {
    return new Tuple2<>(value, System.nanoTime());
  }

  public static <T> Tuple2<T, Long> calcDelta(Tuple2<T, Long> tuple) {
    return new Tuple2<>(tuple.v1, System.nanoTime() - tuple.v2);
  }

  public static <T1, T2> Tuple3<T1, T2, Long> calcDelta(Tuple3<T1, T2, Long> tuple) {
    return new Tuple3<>(tuple.v1, tuple.v2, System.nanoTime() - tuple.v3);
  }

  public static <T, R> Mapper<Tuple2<T, Long>, Tuple2<R, Long>> makeF(Function<T, R> f) {
    return (Tuple2<T, Long> t) -> new Tuple2<>(f.apply(t.v1), t.v2);
  }

  /**
   * SELECT itemid, DOLTOEUR(price), bidderId, bidTime FROM bid;
   */
  public static Query makeQuery1(Source<Bid> bidSource,
      Sink<Tuple> sink) {
    return new Query(
        new TopologyBuilder().streamOf(bidSource).map(Queries::prepare)
            .map(makeF(bid -> new Tuple4<>(bid.auctionId,
                Queries.dollarToEuro(bid.bid), bid.betterId, bid.time))).map(Queries::calcDelta)
            .to(sink).build());
  }

  public static Query makeQueryAgeFilter(Source<Person> personSource, Sink<Tuple> sink) {
    return new Query(
        new TopologyBuilder().streamOf(personSource)
            .map(person -> new Tuple4<>(person.id, person.name, person.province, person.age))
            .filter(p -> Integer.parseInt(p.v4) > 1)
            .to(sink).build());
  }

  public static Query makeSimpleAuctionQuery(Source<Auction> auctionSource, Sink<Tuple> sink) {
    return new Query(
            new TopologyBuilder().streamOf(auctionSource)
                    .map(auction -> new Tuple4<>(auction.id, auction.quantity, auction.currentPrice, auction.reserve))
                    .filter(a -> a.v2 > 5)
                    .to(sink).build());
  }

  /**
   * SELECT itemid, price FROM bid WHERE itemid = 1007 OR itemid = 1020 OR itemid = 2001 OR itemid =
   * 2019 OR itemid = 1087;
   */

  public static Query makeQuery2(Source<Bid> bidSource,
      BenchmarkingSink<Bid> sink) {
    return new Query(new TopologyBuilder().streamOf(bidSource).filter(bid ->
        bid.auctionId == 1007 || bid.auctionId == 1020 || bid.auctionId == 2001 ||
            bid.auctionId == 2019 || bid.auctionId == 1087
    ).to(sink).build());
  }

  /**
   * SELECT person.name, person.city, person.state, open auction.id FROM open auction, person, item
   * WHERE open auction.sellerId = person.id AND person.state = ‘OR’ AND open auction.itemid =
   * item.id AND item.categoryId = 10;
   */
  public static Query makeQuery3(Source<Person> personSource,
      Source<Auction> auctionSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var ps = builder.streamOf(personSource)
        .filter(p -> p.province.equals("Oregon"));
    // ein top builder per query
    return
        builder.streamOf(auctionSource).filter(a -> a.category == 10)
            .window(TumblingWindow.ofEventTime(Time.seconds(5)))
            .join(ps,
                (a, p) -> new Tuple4<>(p.name, p.city, p.province, a.category),
                a -> a.sellerId,
                            p -> p.id, WatermarkGenerator.seconds(0, 1),
                            TimestampExtractor.currentTimeNS()
                    ).to(sink).buildAsQuery();
  }

  /**
   * Select person.name from bid, person where person.id = bid.betterid
   */
  public static Query makePlainJoin(Source<Auction> auctionSource,
          Source<Bid> bidSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var as = builder.streamOf(auctionSource);
    // ein top builder per query
    return
        builder.streamOf(bidSource).window(TumblingWindow.ofEventTime(Time.seconds(10)))
            .join(as, (b, p) -> new Tuple1<>(p.id),
                b -> b.auctionId, p -> p.id,
                WatermarkGenerator.seconds(1, 10_000),
                TimestampExtractor.currentTimeNS()
            ).to(sink).buildAsQuery();
  }

  /**
   * Select person.name from bid, person where person.id = bid.betterid
   */
  public static Query makeAJoin(Source<Person> personSource,
                                Source<Bid> bidSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var ps = builder.streamOf(personSource);
    // ein top builder per query
    return
        builder.streamOf(bidSource)
            .window(TumblingWindow.ofProcessingTime(Time.seconds(5)))
            .ajoin(ps, b -> b.betterId, p -> p.id, (b, p) -> new Tuple1<>(p.name)
            ).to(sink).buildAsQuery();
  }


  private static long dollarToEuro(long dollar) {
    return (long) (1.1 * dollar);
  }
}
