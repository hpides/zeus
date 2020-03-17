import de.hpi.des.hdes.benchmark.GSONSerializer;
import de.hpi.des.hdes.benchmark.generator.InMemoryNexGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.util.StringTokenizer;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.logging.log4j.Level;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Skeleton code for the datastream walkthrough
 */
@Log4j2
public class BenchmarkJob implements Runnable {

  @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "5000")
  private int eventsPerSecond;
  @Option(names = {"--maxDelayInSeconds", "-mds"}, defaultValue = "1")
  private int maxDelayInSeconds;
  @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "10")
  private int timeInSeconds;
  @Option(names = {"--threads", "-t"}, defaultValue = "8")
  private int nThreads;
  @Option(names = {"--personFraction", "-pf"}, defaultValue = "0.05")
  private double personFraction;
  @Option(names = {"--auctionFraction", "-af"}, defaultValue = "0.35")
  private double auctionFraction;
  @Option(names = {"--bidFraction", "-bf"}, defaultValue = "0.60")
  private double bidFraction;
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

  public static void main(final String[] args) {
    new CommandLine(new BenchmarkJob()).execute(args);
  }

  private void makeQuery1(StreamExecutionEnvironment env, InMemoryNexGenerator generator) {
    DataStream<Person> stream = env.fromCollection(generator.getPersonBuffer())
        .name("personStream");

    DataStream<String> alerts = stream
        .map(p -> p.name)
        .name("fraud-detector");

    alerts.print();
  }

  private void makePlainJoin(StreamExecutionEnvironment env) {
    DataStreamSource<String> personSocket = env
        .socketTextStream(generatorHost, personNetworkSocketPort);
    DataStreamSource<String> auctionSocket = env
        .socketTextStream(generatorHost, auctionNetworkSocketPort);
    DataStreamSource<String> bidSocket = env.socketTextStream(generatorHost, bidNetworkSocketPort);

    personSocket.map(e -> GSONSerializer.forPerson().deserialize(e)).setParallelism(1).map(x -> 1)
        .timeWindowAll(Time.seconds(10)).reduce(Integer::sum).print();
    auctionSocket.map(e -> GSONSerializer.forAuction().deserialize(e)).setParallelism(1).map(x -> 1)
        .timeWindowAll(Time.seconds(10)).reduce(Integer::sum).print();
    bidSocket.map(e -> GSONSerializer.forBid().deserialize(e)).setParallelism(1).map(x -> 1)
        .timeWindowAll(Time.seconds(10)).reduce(Integer::sum).print();

//    bidStream.join(auctionStream).where(b -> b.auctionId)
//        .equalTo(a -> a.id).window(
//        TumblingProcessingTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(1))
//        .apply((b, a) -> new Tuple1<Long>(a.id), TypeInformation.of(new TypeHint<Tuple1<Long>>() {
//        })).map(x -> 1)
//        .timeWindowAll(Time.seconds(10)).reduce(Integer::sum).print();

  }

  public static <T> Tuple2<T, Long> calcDelta(Tuple2<T, Long> tuple) {
    return new Tuple2<>(tuple.f0, System.nanoTime() - tuple.f1);
  }

  public static <T1, T2> Tuple3<T1, T2, Long> calcDelta(Tuple3<T1, T2, Long> tuple) {
    return new Tuple3<>(tuple.f0, tuple.f1, System.nanoTime() - tuple.f2);
  }


  public void run() {
    basicBench();
  }

  private void makePlainIntJoin(SingleOutputStreamOperator<Tuple2<Integer, Long>> cs1,
      SingleOutputStreamOperator<Tuple2<Integer, Long>> cs2) {
    SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> s1 = cs1
        .map(new Prepare<>());
    SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> s2 = cs2
        .map(new Prepare<>());
    s1.join(s2).where(new KeySelect()).equalTo(new KeySelect())
        .window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new Join<>())
        .map(new Eject<>()).writeAsCsv("./sink_flink" + System.currentTimeMillis() + ".csv")
        .setParallelism(1);

  }

  private void makePlainIntMap(SingleOutputStreamOperator<Tuple1<Integer>> cs1) {
    cs1
        .map(new Prepare())
        .map(new NoOpMap())
        .map(new CalcDelta2<>())
        .addSink(new CustomSink<>());
  }

  private void basicBench() {
    log.warn("Running with {} EPS, {}s max delay for {}s.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    log.warn("Connecting to {} at {}", generatorHost, basicPort1);
    SingleOutputStreamOperator<Tuple2<Integer, Long>> s1 = env
        .socketTextStream(generatorHost, basicPort1, "\n", -1).map(new Parse())
        .assignTimestampsAndWatermarks(new Timestamper());
    SingleOutputStreamOperator<Tuple2<Integer, Long>> s2 = env
        .socketTextStream(generatorHost, basicPort2, "\n", -1).map(new Parse())
        .assignTimestampsAndWatermarks(new Timestamper());
    log.warn("{}", env.getConfig());

    log.printf(Level.WARN, "Running with %,d EPS, %ds max delay for %ds.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    //makePlainIntMap(s1);
    makePlainIntJoin(s1, s2);
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

  private void nexmarkBench() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    log.printf(Level.WARN, "Running with %,d EPS, %ds max delay for %ds.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);

    makePlainJoin(env);
    try {
      long startTime = System.nanoTime();
      env.execute("Join");
      long endTime = System.nanoTime();
      log.warn("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class Prepare<In> implements MapFunction<Tuple2<In, Long>, Tuple3<In, Long,
      Long>> {

    @Override
    public Tuple3<In, Long, Long> map(Tuple2<In, Long> v) throws Exception {
      return new Tuple3<>(v.f0, v.f1, System.currentTimeMillis());
    }
  }

  private class Eject<In, Other> implements MapFunction<Tuple3<Tuple2<In, Other>, Long, Long>,
      Tuple3<Long, Long, Long>> {

    @Override
    public Tuple3<Long, Long, Long> map(Tuple3<Tuple2<In, Other>, Long, Long> v) throws Exception {
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

  private class NoOpMap implements
      MapFunction<Tuple2<Tuple1<Integer>, Long>, Tuple2<Tuple1<Integer>, Long>> {

    @Override
    public Tuple2<Tuple1<Integer>, Long> map(Tuple2<Tuple1<Integer>, Long> value) throws Exception {
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

  private class Timestamper implements AssignerWithPeriodicWatermarks<Tuple2<Integer, Long>> {

    private long count = 0;

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Tuple2<Integer, Long> element, long previousElementTimestamp) {
      count = (count + 1) % 1_000;
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
}

