import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Skeleton code for the datastream walkthrough
 */
@Log4j2
public class BenchmarkJob implements Runnable {

  @Option(names = {"--basicPort1", "-bsp1"}, defaultValue = "7001")
  private int basicPort1;
  @Option(names = {"--basicPort2", "-bsp2"}, defaultValue = "7002")
  private int basicPort2;
  @Option(names = {"--auctionSourcePort", "-asp"}, defaultValue = "5551")
  private int auctionNetworkSocketPort;
  @Option(names = {"--bidSourcePort", "-bsp"}, defaultValue = "5552")
  private int bidNetworkSocketPort;
  @Option(names = {"--personSourcePort", "-psp"}, defaultValue = "5553")
  private int personNetworkSocketPort;
  @Option(names = {"--generatorHost", "-gh"}, defaultValue = "172.22.17.248")
  private String generatorHost;
  @Option(names = {"--type", "-t"}, defaultValue = "basic")
  private String benchmarkType;
  @Option(names = {"--fixedQueries", "-fq"}, defaultValue = "1")
  private int fixedQueries;
  @Option(names = {"--operator", "-op"}, defaultValue = "map")
  private String operator;

  public static void main(final String[] args) {
    for (String s : args) {
      log.warn(s);
    }
    new CommandLine(new BenchmarkJob()).execute(args);
  }


  public static <T> Tuple2<T, Long> calcDelta(Tuple2<T, Long> tuple) {
    return new Tuple2<>(tuple.f0, System.nanoTime() - tuple.f1);
  }

  public static <T1, T2> Tuple3<T1, T2, Long> calcDelta(Tuple3<T1, T2, Long> tuple) {
    return new Tuple3<>(tuple.f0, tuple.f1, System.nanoTime() - tuple.f2);
  }


  private String formatName(int id, String name) {
    Date date = Calendar.getInstance().getTime();
    DateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
    String strDate = dateFormat.format(date);

    return System.getProperty("user.home") + File.separator + "sink_flink_" + id + "_" + name +
        "_f%"
        + this.fixedQueries + "%" + "_t" + strDate + ".csv";
  }


  public void run() {
    if (this.benchmarkType.equals("basic")) {
      log.info("Running with basic data");
      if (operator.equals("map")) {
        runBasicMaps();
      }
      if (operator.equals("join")) {
        runBasicJoins();
      }
    } else {
      log.info("Running with nexmark data");
      if (operator.equals("1")) {
        runNexmarkFilter();
      }
      if (operator.equals("2")) {
        nexmarkBench(this::makePlainNexmarkJoin, "NARpJoin");
      } //no 3 as no ajoin here
      if (operator.equals("4")) {
        nexmarkBench(this::makeHottestCategoryQuery, "NHotCat");
      }
      if (operator.equals("5")) {
        nexmarkBench(this::makeMaxiumPriceForAuction, "NHighPPA");
      }
    }
  }

  private void runNexmarkFilter() {
    log.warn("Running with {} with filter", benchmarkType);
    StreamExecutionEnvironment env = createEnv();
    System.out.println(env.getCheckpointConfig().toString());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSocket = env
        .socketTextStream(generatorHost, basicPort2, "\n", -1).map(new ParseAuctionNextmark())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    makeNexmarkFilter(auctionSocket).writeAsCsv(formatName(1, "NARFilter"))
        .setParallelism(1);
    for (int i = 1; i < this.fixedQueries; i++) {
      makeNexmarkFilter(auctionSocket).addSink(new DiscardingSink<>());
    }
    try {
      long startTime = System.nanoTime();
      env.execute("Join");
      long endTime = System.nanoTime();
      log.warn("Results might be scatterd as flink parallelizes sinks");
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makeIntMap(
      SingleOutputStreamOperator<Tuple2<Integer, Long>> cs1) {

    return cs1
        .assignTimestampsAndWatermarks(new GenericTimestamper<>())
        .map(new Prepare<>()).map(new NoOpMap<>()).map(new Eject<>());
  }


  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makePlainIntJoin(
      SingleOutputStreamOperator<Tuple2<Integer, Long>> cs1,
      SingleOutputStreamOperator<Tuple2<Integer, Long>> cs2) {

    SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> s1 = cs1
        .map(new Prepare<>());
    SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> s2 = cs2

        .map(new Prepare<>());
    return s1.join(s2).where(new KeySelect()).equalTo(new KeySelect())
        .window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new Join<>())
        .map(new Eject<>());
  }

  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makePlainNexmarkJoin(
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidStream,
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionStream) {
    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Long, Integer, Integer>, Long, Long>> s1 = bidStream
        .map(new Prepare<>());

    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long>> s2 = auctionStream
        .map(new Prepare<>());
    return s1.join(s2).where(new KeyBidAuctionIdSelect()).equalTo(new KeyAuctionAuctionIdSelect())
        .window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new Join<>())
        .map(new Eject<>());
  }

  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makeNexmarkFilter(
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionStream) {

    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long>> s2 = auctionStream
        .map(new Prepare<>());
    return s2.filter(t2 -> t2.f0.f3 > 5000)
        .map(new Eject<>());
  }


  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makeHottestCategoryQuery(
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidStream,
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionStream) {

    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Long, Integer, Integer>, Long, Long>> s1 = bidStream
        .map(new Prepare<>());
    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long>> s2 = auctionStream
        .map(new Prepare<>());
    return s1.join(s2).where(new KeyBidAuctionIdSelect()).equalTo(new KeyAuctionAuctionIdSelect())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new Join<>())
        .keyBy(new KeyJoinedBidAuctionTypeSelect())
        .timeWindow(Time.seconds(5))
        .aggregate(new CountTimestampAggregator())
        .timeWindowAll(Time.seconds(5))
        .max(0)
        .map(new Eject<>());

  }

  private SingleOutputStreamOperator<Tuple3<Long, Long, Long>> makeMaxiumPriceForAuction(
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidStream,
      SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionStream
  ) {
    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Long, Integer, Integer>, Long, Long>> s1 = bidStream
        .map(new Prepare<>());
    SingleOutputStreamOperator<Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long>> s2 = auctionStream
        .map(new Prepare<>());
    return s1.join(s2).where(new KeyBidAuctionIdSelect()).equalTo(new KeyAuctionAuctionIdSelect())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new Join<>())
        .filter(t -> t.f0.f0.f3 > t.f0.f1.f3)
        .map(t -> new Tuple4<>(t.f0.f1.f0, t.f0.f1.f3, t.f1, t.f2))
        .returns(new TypeHint<Tuple4<Long, Integer, Long, Long>>() {
        })
        .keyBy(new KeyJoinedBidAuctionIdSelect())
        .timeWindow(Time.seconds(5))
        .max(1)
        .map(t -> new Tuple3<Long, Long, Long>(t.f0, t.f2, t.f3))
        .returns(new TypeHint<Tuple3<Long, Long, Long>>() {
        })
        .map(new Eject<>());

  }

  private void runBasicJoins() {
    log.warn("Running with {}", benchmarkType);
    StreamExecutionEnvironment env = createEnv();
    System.out.println(env.getCheckpointConfig().toString());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    log.warn("Connecting to {} at {}", generatorHost, basicPort1);
    SingleOutputStreamOperator<Tuple2<Integer, Long>> s1 = env
        .socketTextStream(generatorHost, basicPort1, "\n", -1).map(new Parse())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    SingleOutputStreamOperator<Tuple2<Integer, Long>> s2 = env
        .socketTextStream(generatorHost, basicPort2, "\n", -1).map(new Parse())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    log.warn("{}", env.getConfig());
    makePlainIntJoin(s1, s2).writeAsCsv(formatName(1, "join"))
        .setParallelism(1);
    for (int i = 1; i < this.fixedQueries; i++) {
      makePlainIntJoin(s1, s2).addSink(new DiscardingSink<>());
    }

    try {
      long startTime = System.nanoTime();
      env.execute("Join");
      long endTime = System.nanoTime();
      log.warn("Results might be scatterd as flink parallelizes sinks");
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void runBasicMaps() {
    log.warn("Running with {}", benchmarkType);
    StreamExecutionEnvironment env = createEnv();
    System.out.println(env.getCheckpointConfig().toString());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    log.warn("Connecting to {} at {}", generatorHost, basicPort1);
    SingleOutputStreamOperator<Tuple2<Integer, Long>> s1 = env
        .socketTextStream(generatorHost, basicPort1, "\n", -1).map(new Parse())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    log.warn("{}", env.getConfig());
    makeIntMap(s1).writeAsCsv(formatName(1, "map"))
        .setParallelism(1);
    for (int i = 1; i < this.fixedQueries; i++) {
      makeIntMap(s1).addSink(new DiscardingSink<>());
    }

    try {
      long startTime = System.nanoTime();
      env.execute("Map");
      long endTime = System.nanoTime();
      log.warn("Results might be scatterd as flink parallelizes sinks");
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private void nexmarkBench(BiFunction<SingleOutputStreamOperator<Tuple2<Tuple4<Long, Long,
      Integer, Integer>, Long>>, SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer,
      Integer>, Long>>, SingleOutputStreamOperator<Tuple3<Long, Long, Long>>> makeQuery,
      String name) {
    log.warn("Running with {} with {}", benchmarkType, name);
    StreamExecutionEnvironment env = createEnv();
    System.out.println(env.getCheckpointConfig().toString());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    log.warn("Connecting to {} at {}", generatorHost, basicPort1);
    SingleOutputStreamOperator<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> bidSocket = env
        .socketTextStream(generatorHost, basicPort1, "\n", -1).map(new ParseBidNextmark())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    SingleOutputStreamOperator<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> auctionSocket = env
        .socketTextStream(generatorHost, basicPort2, "\n", -1).map(new ParseAuctionNextmark())
        .assignTimestampsAndWatermarks(new GenericTimestamper<>());
    log.warn("{}", env.getConfig());

    makeQuery.apply(bidSocket, auctionSocket).writeAsCsv(formatName(1, name))
        .setParallelism(1);
    for (int i = 1; i < this.fixedQueries; i++) {
      makeQuery.apply(bidSocket, auctionSocket).addSink(new DiscardingSink<>());
    }

    try {
      long startTime = System.nanoTime();
      env.execute(name);
      long endTime = System.nanoTime();
      log.warn("Results might be scatterd as flink parallelizes sinks");
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private StreamExecutionEnvironment createEnv() {
    Configuration conf = new Configuration();
    conf.setString("taskmanager.memory.managed.size", MemorySize.parse("8g").toString());
    conf.setString("taskmanager.memory.network.max", MemorySize.parse("2g").toString());
    return StreamExecutionEnvironment.createLocalEnvironment(Runtime.getRuntime().availableProcessors(), conf);
  }

  private class Prepare<In> implements MapFunction<Tuple2<In, Long>, Tuple3<In, Long,
      Long>> {

    @Override
    public Tuple3<In, Long, Long> map(Tuple2<In, Long> v) throws Exception {
      return new Tuple3<>(v.f0, v.f1, System.currentTimeMillis());
    }
  }

  private class Eject<E> implements MapFunction<Tuple3<E, Long, Long>, Tuple3<Long, Long, Long>> {

    @Override
    public Tuple3<Long, Long, Long> map(Tuple3<E, Long, Long> v) throws Exception {
      return new Tuple3<>(v.f1, v.f2, System.currentTimeMillis());
    }
  }

  private class CalcDelta<In, Other> implements
      MapFunction<Tuple3<In, Other, Long>, Tuple3<In, Other, Long>> {

    @Override
    public Tuple3<In, Other, Long> map(Tuple3<In, Other, Long> tuple) throws Exception {
      return new Tuple3<>(tuple.f0, tuple.f1, System.nanoTime() - tuple.f2);
    }


  }

  private class CalcDelta2<In> implements MapFunction<Tuple2<In, Long>, Tuple2<In, Long>> {

    @Override
    public Tuple2<In, Long> map(Tuple2<In, Long> tuple) throws Exception {
      return new Tuple2<>(tuple.f0, System.nanoTime() - tuple.f1);
    }
  }

  private class KeySelect implements KeySelector<Tuple3<Integer, Long, Long>, Integer> {

    @Override
    public Integer getKey(Tuple3<Integer, Long, Long> value) throws Exception {
      return value.f0;
    }
  }

  private class KeyBidAuctionIdSelect implements
      KeySelector<Tuple3<Tuple4<Long, Long, Integer, Integer>, Long, Long>, Long> {

    @Override
    public Long getKey(Tuple3<Tuple4<Long, Long, Integer, Integer>, Long, Long> value)
        throws Exception {
      return value.f0.f1;
    }
  }

  private class KeyAuctionAuctionIdSelect implements
      KeySelector<Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long>, Long> {

    @Override
    public Long getKey(Tuple3<Tuple4<Long, Integer, Integer, Integer>, Long, Long> value)
        throws Exception {
      return value.f0.f0;
    }
  }

  private class KeyJoinedBidAuctionTypeAndReducePrepSelect implements
      KeySelector<Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long>, Integer> {

    @Override
    public Integer getKey(
        Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> value)
        throws Exception {
      return value.f0.f0.f1.f2;
    }
  }

  private class KeyJoinedBidAuctionTypeSelect implements
      KeySelector<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Integer> {

    @Override
    public Integer getKey(
        Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long> value)
        throws Exception {
      return value.f0.f1.f2;
    }
  }

  private class KeyJoinedBidAuctionIdSelect implements
      KeySelector<Tuple4<Long, Integer, Long, Long>, Long> {

    @Override
    public Long getKey(Tuple4<Long, Integer, Long, Long> value) throws Exception {
      return value.f0;
    }
  }

  private class Join<In, Other> implements JoinFunction<Tuple3<In, Long, Long>,
      Tuple3<Other, Long, Long>, Tuple3<Tuple2<In, Other>, Long, Long>> {

    @Override
    public Tuple3<Tuple2<In, Other>, Long, Long> join(Tuple3<In, Long, Long> first,
        Tuple3<Other, Long, Long> second)
        throws Exception {
      return new Tuple3<>(new Tuple2<>(first.f0, second.f0),
          Math.max(first.f1, second.f1), Math.max(first.f2, second.f2));
    }
  }

  private class NoOpMap<E> implements
      MapFunction<Tuple3<E, Long, Long>, Tuple3<E, Long, Long>> {

    @Override
    public Tuple3<E, Long, Long> map(Tuple3<E, Long, Long> value) throws Exception {
      return value;
    }
  }

  private class Parse implements MapFunction<String, Tuple2<Integer, Long>> {

    @Override
    public Tuple2<Integer, Long> map(String value) throws Exception {
      StringTokenizer vals = new StringTokenizer(value, ",");
      return new Tuple2<>(Integer.parseInt(vals.nextToken()), Long.parseLong(vals.nextToken()));
    }
  }

  private class ParseBidNextmark implements
      MapFunction<String, Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> {

    @Override
    public Tuple2<Tuple4<Long, Long, Integer, Integer>, Long> map(String value) throws Exception {
      StringTokenizer vals = new StringTokenizer(value, ",");
      return new Tuple2<>(
          new Tuple4<>(Long.parseLong(vals.nextToken()), Long.parseLong(vals.nextToken()),
              Integer.parseInt(vals.nextToken()), Integer.parseInt(vals.nextToken())),
          Long.parseLong(vals.nextToken()));
    }
  }

  private class ParseAuctionNextmark implements
      MapFunction<String, Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> {

    @Override
    public Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long> map(String value)
        throws Exception {
      StringTokenizer vals = new StringTokenizer(value, ",");
      return new Tuple2<>(
          new Tuple4<>(Long.parseLong(vals.nextToken()), Integer.parseInt(vals.nextToken()),
              Integer.parseInt(vals.nextToken()), Integer.parseInt(vals.nextToken())),
          Long.parseLong(vals.nextToken()));
    }
  }

  private class GenericTimestamper<E> implements AssignerWithPeriodicWatermarks<Tuple2<E, Long>> {

    private long count = 0;

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Tuple2<E, Long> element, long previousElementTimestamp) {
      count = (count + 1) % 100_000;
      if (count == 0) {
        currentMaxTimestamp = element.f1;
      }
      return element.f1;
    }

    @Override
    public Watermark getCurrentWatermark() {
      // return the watermark as current highest timestamp
      return new Watermark(this.currentMaxTimestamp);
    }
  }

  private class CountTimestampAggregator implements
      AggregateFunction<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

    @Override
    public Tuple3<Long, Long, Long> createAccumulator() {
      return new Tuple3<>(0L, 0L, 0L);
    }

    @Override
    public Tuple3<Long, Long, Long> add(
        Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long> t1,
        Tuple3<Long, Long, Long> state) {
      long timestamp1 = Math.max(t1.f1, state.f1);
      long timestamp2 = Math.max(t1.f2, state.f2);
      return new Tuple3<>(state.f0 + 1, timestamp1, timestamp2);
    }

    @Override
    public Tuple3<Long, Long, Long> getResult(Tuple3<Long, Long, Long> state) {
      return state;
    }

    @Override
    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> state1,
        Tuple3<Long, Long, Long> state2) {
      long timestamp1 = Math.max(state1.f1, state2.f1);
      long timestamp2 = Math.max(state1.f2, state2.f2);
      return new Tuple3<>(state1.f0 + state2.f0, timestamp1, timestamp2);
    }
  }

  private class CountReducer implements
      ReduceFunction<Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long>> {

    @Override
    public Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> reduce(
        Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> t0,
        Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> t1)
        throws Exception {
      return new Tuple2<>(t0.f0, t0.f1 + t1.f1);
    }
  }

  private class MaxReducer implements
      ReduceFunction<Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long>> {

    @Override
    public Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> reduce(
        Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> t0,
        Tuple2<Tuple3<Tuple2<Tuple4<Long, Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>>, Long, Long>, Long> t1)
        throws Exception {
      return new Tuple2<>(t0.f0, t0.f1 + t1.f1);
    }
  }


}

