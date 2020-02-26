import de.hpi.des.hdes.benchmark.BlockingSource;
import de.hpi.des.hdes.benchmark.generator.InMemoryNexGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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

  private void makePlainJoin(StreamExecutionEnvironment env, InMemoryNexGenerator generator) {
    DataStream<Bid> bidStream = env.addSource(
        new CustomSource<Bid>(generator.getBidBuffer(),
            (int) (this.bidFraction * this.eventsPerSecond), this.timeInSeconds),
        TypeInformation.of(Bid.class))
        .name("bid stream");

    DataStream<Auction> auctionStream = env.addSource(
        new CustomSource<Auction>(generator.getAuctionBuffer(),
            (int) (this.auctionFraction * this.eventsPerSecond), this.timeInSeconds),
        TypeInformation.of(Auction.class))
        .name("auction stream");

    bidStream.join(auctionStream).where(b -> b.auctionId)
        .equalTo(a -> a.id).window(
        TumblingProcessingTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(1))
        .apply((b, a) -> new Tuple1<Long>(a.id), TypeInformation.of(new TypeHint<Tuple1<Long>>() {
        })).map(x -> 1)
        .timeWindowAll(Time.seconds(10)).reduce(Integer::sum).print();

  }

  public static <T> Tuple2<T, Long> calcDelta(Tuple2<T, Long> tuple) {
    return new Tuple2<>(tuple.f0, System.nanoTime() - tuple.f1);
  }

  public static <T1, T2> Tuple3<T1, T2, Long> calcDelta(Tuple3<T1, T2, Long> tuple) {
    return new Tuple3<>(tuple.f0, tuple.f1, System.nanoTime() - tuple.f2);
  }

  public static <In, Other> JoinFunction<Tuple2<In, Long>, Tuple2<Other, Long>, Tuple3<In, Other, Long>> makeJoinF() {
    return new JoinFunction<Tuple2<In, Long>, Tuple2<Other, Long>, Tuple3<In, Other, Long>>() {
      @Override
      public Tuple3<In, Other, Long> join(Tuple2<In, Long> first, Tuple2<Other, Long> second)
          throws Exception {
        return new Tuple3<>(first.f0, second.f0,
            Math.max(first.f1, second.f1));
      }
    };
  }

  public void run() {
    basicBench();
  }

  private void makePlainIntJoin(StreamExecutionEnvironment env, IntCustomSource cs1,
      IntCustomSource cs2) {
    DataStream<Tuple2<Tuple1<Integer>, Long>> s1 = env.addSource(cs1).setParallelism(1)
        .map(new Prepare());
    DataStream<Tuple2<Tuple1<Integer>, Long>> s2 = env.addSource(cs2).setParallelism(1)
        .map(new Prepare());
    s1.join(s2).where(new KeySelect()).equalTo(new KeySelect())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply(makeJoinF())
        .map(new CalcDelta<>()).addSink(new CustomSink<>());

  }

  private void makePlainIntMap(StreamExecutionEnvironment env, IntCustomSource cs1) {
    env.addSource(cs1).setParallelism(1).map(new Prepare()).map(e -> e).map(new CalcDelta2<>())
        .addSink(new CustomSink<>());
  }

  private void basicBench() {
    log.info("Running with {} EPS, {}s max delay for {}s.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    IntCustomSource cs1 = new IntCustomSource(eventsPerSecond, timeInSeconds, new Random(1));
    IntCustomSource cs2 = new IntCustomSource(eventsPerSecond, timeInSeconds, new Random(2));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    log.printf(Level.WARN, "Running with %,d EPS, %ds max delay for %ds.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);

    //makePlainIntJoin(env, cs1, cs2);
    makePlainIntMap(env, cs1);
    makePlainIntJoin(env, cs1, cs2);
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
    final BlockingSource<Person> personSource = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final BlockingSource<Auction> auctionSource = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final BlockingSource<Bid> bidSource = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    final InMemoryNexGenerator generator = new InMemoryNexGenerator(auctionSource, bidSource,
        personSource,
        eventsPerSecond,
        timeInSeconds, this.personFraction, this.auctionFraction, this.bidFraction);
    try {
      generator.prepare().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    log.printf(Level.WARN, "Running with %,d EPS, %ds max delay for %ds.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);

    makePlainJoin(env, generator);
    try {
      long startTime = System.nanoTime();
      env.execute("Join");
      long endTime = System.nanoTime();
      log.warn("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class Prepare implements MapFunction<Tuple1<Integer>, Tuple2<Tuple1<Integer>, Long>> {

    @Override
    public Tuple2<Tuple1<Integer>, Long> map(Tuple1<Integer> v) throws Exception {
      return new Tuple2<>(v, System.nanoTime());
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

  private class KeySelect implements KeySelector<Tuple2<Tuple1<Integer>, Long>, Integer> {

    @Override
    public Integer getKey(Tuple2<Tuple1<Integer>, Long> value) throws Exception {
      return value.f0.f0;
    }
  }
}