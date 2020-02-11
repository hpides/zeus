package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.assigner.TumblingEventTimeWindow;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple4;

public class Queries {

  /**
   * Nooop query
   *
   * @param source
   * @param sink
   * @return
   */
  public static <T> Query makeQuery0(BlockingSource<T> source, Sink<T> sink) {
    return new Query(
        new TopologyBuilder().streamOf(source).filter(e -> true).to(sink).build());
  }


  /**
   * SELECT itemid, DOLTOEUR(price), bidderId, bidTime FROM bid;
   */
  public static Query makeQuery1(BlockingSource<Bid> bidSource,
      Sink<Tuple> sink) {
    return new Query(
        new TopologyBuilder().streamOf(bidSource).map(bid -> new Tuple4<>(bid.auctionId,
            Queries.dollarToEuro(bid.bid), bid.betterId, bid.time)).to(sink).build());
  }

  public static Query makeQueryAgeFilter(BlockingSource<Person> personSource, Sink<Tuple> sink) {
    return new Query(
        new TopologyBuilder().streamOf(personSource)
            .map(person -> new Tuple4<>(person.id, person.name, person.address, person.profile.age))
            .filter(p -> Integer.parseInt(p.v4) > 1)
            .to(sink).build());
  }

  /**
   * SELECT itemid, price FROM bid WHERE itemid = 1007 OR itemid = 1020 OR itemid = 2001 OR itemid =
   * 2019 OR itemid = 1087;
   */

  public static Query makeQuery2(BlockingSource<Bid> bidSource,
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
  public static Query makeQuery3(BlockingSource<Person> personSource,
      BlockingSource<Auction> auctionSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var ps = builder.streamOf(personSource)
        .filter(p -> p.address.province.equals("Oregon"));
    // ein top builder per query
    return
        builder.streamOf(auctionSource).filter(a -> a.category == 10).windowAll()
            .join(ps, (a, p) -> new Tuple4<>(p.name,
                    p.address.city, p.address.province, a.category),
                (a, p) -> a.sellerId == p.id
            ).to(sink).buildAsQuery();
  }

  /**
   * Select person.name from bid, person where person.id = bid.betterid
   */
  public static Query makePlainJoin(BlockingSource<Person> personSource,
      BlockingSource<Bid> bidSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var ps = builder.streamOf(personSource);
    // ein top builder per query
    return
        builder.streamOf(bidSource).windowAll()
            .join(ps, (b, p) -> new Tuple1<>(p.name),
                (b, p) -> true
            ).to(sink).buildAsQuery();
  }

  /**
   * Select person.name from bid, person where person.id = bid.betterid
   */
  public static Query makeAJoin(BlockingSource<Person> personSource,
      BlockingSource<Bid> bidSource, BenchmarkingSink<Tuple> sink) {
    var builder = new TopologyBuilder();
    var ps = builder.streamOf(personSource);
    // ein top builder per query
    return
        builder.streamOf(bidSource).window(TumblingEventTimeWindow.ofProcessingTime(Time.seconds(5)))
            .ajoin(ps, b -> b.betterId, p -> p.id, (b, p) -> new Tuple1<>(p.name)
            ).to(sink).buildAsQuery();
  }


  private static long dollarToEuro(long dollar) {
    return (long) (1.1 * dollar);
  }
}
