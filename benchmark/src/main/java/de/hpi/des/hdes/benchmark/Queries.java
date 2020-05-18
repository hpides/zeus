package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.Filter;
import de.hpi.des.hdes.engine.udf.Join;
import de.hpi.des.hdes.engine.udf.Mapper;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;

import java.util.function.Function;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;

public class Queries {

  private Queries() {
  }

  // Microbenchmark Queries
  public static Query makeFilter0Measured(Source<Tuple2<Integer, Long>> source, Sink<Tuple> sink) {
    return new TopologyBuilder().streamOf(source).map(t -> t.v1).filter(e -> e % 2 == 0).flatProfiling().to(sink)
        .buildAsQuery();
  }

  /**
   * Nooop query
   */
  public static <T> Query makeQuery0(Source<T> source, Sink<T> sink) {
    return new Query(
        new TopologyBuilder().streamOf(source).map(e -> e).to(sink).build());
  }

  public static <T> Query makeQuery0Measured(Source<Tuple2<T, Long>> source, Sink<Tuple> sink) {
    return
        new TopologyBuilder().streamOf(source).map(Queries::prepare).map(e -> e)
            .map(Queries::setEjectTimestamp).to(sink).buildAsQuery();
  }

  public static <In, Other> Join<Tuple3<In, Long, Long>, Tuple3<Other, Long, Long>, Tuple3<Tuple2<In, Other>, Long, Long>> makeJoinF() {
    return (Tuple3<In, Long, Long> t1, Tuple3<Other, Long, Long> t2) -> new Tuple3<>(
        new Tuple2<>(t1.v1, t2.v1),
        Math.max(t1.v2, t2.v2), Math.max(t1.v3, t2.v3));
  }

  public static <T1, T2> Query makePlainJoin0Measured(Source<Tuple2<T1, Long>> source1,
      Source<Tuple2<T2, Long>> source2, Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(source1).map(Queries::prepare);
    var s2 = tp.streamOf(source2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .join(s2, makeJoinF(), t1 -> t1.v1, t2 -> t2.v1, WatermarkGenerator.seconds(0, 1_000),
            TimestampExtractor.currentTimeNS());
    return j1.map(Queries::setEjectTimestamp).to(sink).buildAsQuery();
  }

  public static Query makeNexmarkLightPlainJoinMeasured(
      Source<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSource1,
      Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource2,
      Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(bidSource1).map(Queries::prepare);
    var s2 = tp.streamOf(auctionSource2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .join(s2, makeJoinF(), t1 -> t1.v1.v2, t2 -> t2.v1.v1, WatermarkGenerator.seconds(0, 1_000),
            TimestampExtractor.currentTimeNS());
    return j1.map(Queries::setEjectTimestamp).to(sink).buildAsQuery();
  }

  public static Query makeNexmarkLightAJoinMeasured(
      Source<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSource1,
      Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource2,
      Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(bidSource1).map(Queries::prepare);
    var s2 = tp.streamOf(auctionSource2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .ajoin(s2, t1 -> t1.v1.v2, t2 -> t2.v1.v1, makeJoinF(),"ajoin");
    return j1.map(Queries::setEjectTimestamp).to(sink).buildAsQuery();
  }

  public static Query makeNexmarkLightFilterMeasured(
      Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource,
      Sink<Tuple> auctionSink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(auctionSource)
        .map(Queries::prepare)
        .filter(t2 -> t2.v1.v4 > 5000)
        .map(Queries::setEjectTimestamp)
        .to(auctionSink)
        .buildAsQuery();

    return s1;
  }

  public static Query makeNexmarkLightAJoinCapacityMeasured(
    Source<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSource1,
    Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource2, Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(bidSource1).map(Queries::prepare);
    var s2 = tp.streamOf(auctionSource2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5))).ajoin(s2, t1 -> t1.v1.v2, t2 -> t2.v1.v1,
        makeJoinF(), (e) -> System.nanoTime(), new WatermarkGenerator<>(0, 1), "ajoin");
    return j1.map(Queries::setEjectTimestamp).window(TumblingWindow.ofEventTime(Time.seconds(5))).aggregate(
        new Aggregator<Tuple3<Long, Long, Long>, Tuple4<Integer, Double, Double, Double>, Tuple4<Integer, Double, Double, Double>>() {
          @Override
          public Tuple4<Integer, Double, Double, Double> initialize() {
            return new Tuple4<Integer, Double, Double, Double>(0, 0.0, 0.0, 0.0);
          }

          @Override
          public Tuple4<Integer, Double, Double, Double> add(Tuple4<Integer, Double, Double, Double> state,
              Tuple3<Long, Long, Long> input) {
            int old_count = state.v1;
            int new_count = state.v1 + 1;
            return new Tuple4<Integer, Double, Double, Double>(new_count,
                (double) (((state.v2 * old_count) + (int) (input.v3 - input.v1)) / new_count),
                (double) (((state.v3 * old_count) + (int) (input.v3 - input.v2)) / new_count),
                (double) (((state.v4 * old_count) + (int) (input.v2 - input.v1)) / new_count));
          }

          @Override
          public Tuple4<Integer, Double, Double, Double> getResult(Tuple4<Integer, Double, Double, Double> state) {
            return state;
          }
        }, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime()).to(sink).buildAsQuery();
  }

  public static Query makeNexmarkHottestCategory(
      Source<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSource1,
      Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource2,
      Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(bidSource1).map(Queries::prepare);
    var s2 = tp.streamOf(auctionSource2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .ajoin(s2, t1 -> t1.v1.v2, t2 -> t2.v1.v1, makeJoinF(), e -> System.nanoTime(),
            new WatermarkGenerator<>(0, 10), "ajoinHottestCategory");
    return j1.window(TumblingWindow.ofEventTime(Time.seconds(5))).groupBy(t1 -> t1.v1.v2.v3)
        .aggregate(new Aggregator<
            Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>,
            Tuple3<Integer, Long, Long>,
            Tuple3<Integer, Long, Long>>() {

          @Override
          public Tuple3<Integer, Long, Long> initialize() {
            return new Tuple3<>(0, 0L, 0L);
          }

          @Override
          public Tuple3<Integer, Long, Long> add(Tuple3<Integer, Long, Long> state,
              Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long> input) {
            return new Tuple3<>(state.v1 + 1, Math.max(state.v2, input.v2), Math.max(state.v3, input.v3));
          }

          @Override
          public Tuple3<Integer, Long, Long> getResult(Tuple3<Integer, Long, Long> state) {
            return state;
          }
        }, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime())
        .window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .aggregate(new Aggregator<
            Tuple3<Integer, Long, Long>,
            Tuple3<Integer, Long, Long>,
            Tuple3<Integer, Long, Long>>() {

          @Override
          public Tuple3<Integer, Long, Long> initialize() {
            return new Tuple3<>(0, 0L, 0L);
          }

          @Override
          public Tuple3<Integer, Long, Long> add(Tuple3<Integer, Long, Long> state,
              Tuple3<Integer, Long, Long> input) {
            if(state.v1 < input.v1) {
              return input;
            }
            else {
              return state;
            }
          }

          @Override
          public Tuple3<Integer, Long, Long> getResult(Tuple3<Integer, Long, Long> state) {
            return state;
          }
        }, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime())
        .map(Queries::setEjectTimestamp)
        .to(sink).buildAsQuery();
  }

  public static Query makeNexmarkMaxiumPriceForAuction(
      Source<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSource1,
      Source<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSource2,
      Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(bidSource1).map(Queries::prepare);
    var s2 = tp.streamOf(auctionSource2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .ajoin(s2, t1 -> t1.v1.v2, t2 -> t2.v1.v1, makeJoinF(), e -> System.nanoTime(),
            new WatermarkGenerator<>(0, 10), "ajoinMaximumPrice");
    return j1
        .filter(t2 -> t2.v1.v1.v4 > t2.v1.v2.v4)
        .window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .groupBy(t1 -> t1.v1.v2.v1)
        .aggregate(new Aggregator<
            Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>,
            Tuple3<Integer, Long, Long>,
            Tuple3<Integer, Long, Long>>() {

          @Override
          public Tuple3<Integer, Long, Long> initialize() {
            return new Tuple3<>(0, 0L, 0L);
          }

          @Override
          public Tuple3<Integer, Long, Long> add(Tuple3<Integer, Long, Long> state,
              Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long> input) {
            if(input.v1.v1.v4 > state.v1) {
              return new Tuple3<>(input.v1.v1.v4, Math.max(state.v2, input.v2), Math.max(state.v3, input.v3));
            }
            else {
              return new Tuple3<>(state.v1, Math.max(state.v2, input.v2), Math.max(state.v3, input.v3));
            }
          }

          @Override
          public Tuple3<Integer, Long, Long> getResult(Tuple3<Integer, Long, Long> state) {
            return state;
          }
        }, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime())
        .map(Queries::setEjectTimestamp)
        .to(sink).buildAsQuery();
  }

  public static <T1, T2> Query makeAJoin0Measured(Source<Tuple2<T1, Long>> source1,
      Source<Tuple2<T2, Long>> source2, Sink<Tuple> sink) {
    var tp = new TopologyBuilder();

    var s1 = tp.streamOf(source1).map(Queries::prepare);
    var s2 = tp.streamOf(source2).map(Queries::prepare);
    var j1 = s1.window(TumblingWindow.ofEventTime(Time.seconds(5)))
        .ajoin(s2, t1 -> t1.v1, t2 -> t2.v1, makeJoinF(), "join0measured");
    return j1.map(Queries::setEjectTimestamp).to(sink).buildAsQuery();
  }


  //value, event time, processing time
  public static <T> Tuple3<T, Long, Long> prepare(Tuple2<T, Long> value) {
    return new Tuple3<>(value.v1, value.v2, System.currentTimeMillis());
  }

  public static <T> Tuple2<T, Long> calcDelta(Tuple2<T, Long> tuple) {
    return new Tuple2<>(tuple.v1, System.nanoTime() - tuple.v2);
  }

  public static <T> Tuple3<Long, Long, Long> setEjectTimestamp(Tuple3<T, Long, Long> tuple) {
    return new Tuple3<>(tuple.v2, tuple.v3, System.currentTimeMillis());
  }

  public static <T1, T2> Tuple3<T1, T2, Long> calcDelta(Tuple3<T1, T2, Long> tuple) {
    return new Tuple3<>(tuple.v1, tuple.v2, System.nanoTime() - tuple.v3);
  }

  public static <T, R> Mapper<Tuple2<T, Long>, Tuple2<R, Long>> makeMap(Function<T, R> f) {
    return (Tuple2<T, Long> t) -> new Tuple2<>(f.apply(t.v1), t.v2);
  }

  public static <T, R> Filter<Tuple2<T, Long>> makeFilter(Function<T, Boolean> f) {
    return (Tuple2<T, Long> t) -> f.apply(t.v1);
  }

}
