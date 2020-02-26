package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.Generator;
import de.hpi.des.hdes.benchmark.generator.InMemoryNexGenerator;
import de.hpi.des.hdes.benchmark.generator.IntegerTupleGenerator;
import de.hpi.des.hdes.benchmark.generator.NetworkGenerator;
import de.hpi.des.hdes.benchmark.generator.StringTupleGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.Engine;
import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingEventTimeWindow;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Log4j2
public class Main implements Runnable {

  @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "100000")
  private int eventsPerSecond;
  @Option(names = {"--maxDelayInSeconds", "-mds"}, defaultValue = "10")
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
    new CommandLine(new Main()).execute(args);
  }

  @Override
  public void run() {
    runNexmarkLocalhostNetwork();
  }

  public void runNexmark() {
    final var personSource = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final var auctionSource = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final var bidSource = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    final var generator = new InMemoryNexGenerator(auctionSource, bidSource, personSource,
        eventsPerSecond,
        timeInSeconds, this.personFraction, this.auctionFraction, this.bidFraction);
    var prepared = generator.prepare();
    try {
      log.info("Preparing");
      prepared.get();
      log.info("Done preparing");
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    log.printf(Level.INFO, "Running with %,d EPS, %ds max delay for %ds.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    long startTime = System.nanoTime();
    var q0Sink = new BenchmarkingSink<Person>();
    var q0 = Queries.makeQuery0(personSource, q0Sink);
    var q00Sink = new BenchmarkingSink<Bid>();
    var q00 = Queries.makeQuery0(bidSource, q00Sink);
    var q1Sink = new BenchmarkingSink<Tuple>();
    var q1 = Queries.makeQuery1(bidSource, q1Sink);
    var q2Sink = new BenchmarkingSink<Bid>();
    var q2 = Queries.makeQuery2(bidSource, q2Sink);
    var q3Sink = new BenchmarkingSink<Tuple>();
    var q3 = Queries.makeQuery3(personSource, auctionSource, q3Sink);

    var q4Sink = new BenchmarkingSink<Tuple>();
    var q4 = Queries.makePlainJoin(auctionSource, bidSource, q4Sink);
    var q5Sink = new BenchmarkingSink<Tuple>();
    var q5 = Queries.makePlainJoin(auctionSource, bidSource, q5Sink);

    var querySinks = List.of(q0Sink, q00Sink, q1Sink, q2Sink, q3Sink, q4Sink, q5Sink);
    var jobManager = new JobManager();
    var done = generator.generate();

//    jobManager.addQuery(q0);
//    jobManager.addQuery(q00);
    //jobManager.addQuery(q1);
    //jobManager.addQuery(q2);
    //jobManager.addQuery(q3);

    jobManager.addQuery(q1);
    //jobManager.addQuery(q5);
    jobManager.runEngine();

    //jobManager.deleteQuery(q2, 10, ChronoUnit.SECONDS);
    //jobManager.deleteQuery(q5, 10, ChronoUnit.SECONDS);

    try {
      done.get();
      long endTime = System.nanoTime();
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
      Thread.sleep(TimeUnit.SECONDS.toMillis(maxDelayInSeconds));

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    jobManager.shutdown();
    querySinks.forEach(qs -> {
      log.info("Latency {} Milliseconds", qs.getProcessingLatency());
      log.printf(Level.INFO, "Total Tuples %,d", qs.getTotalCount());
    });
  }

  public void runNexmarkLocalhostNetwork() {
    final String engineIp = "127.0.0.1";
    final int auctionNetworkSocketPort = 5551;
    final int bidNetworkSocketPort = 5552;
    final int personNetworkSocketPort = 5553;

    // Be aware that the sources are not flushed and could therefore still have events in their buffer when execution is finished
    final var auctionSource = new NetworkSource<>((int) (auctionFraction * eventsPerSecond * maxDelayInSeconds), auctionNetworkSocketPort, Auction.class);
    final var bidSource = new NetworkSource<>((int) (bidFraction * eventsPerSecond * maxDelayInSeconds), bidNetworkSocketPort, Bid.class);
    final var personSource = new NetworkSource<>((int) (personFraction * eventsPerSecond * maxDelayInSeconds), personNetworkSocketPort, Person.class);

    Thread t1 = new Thread(auctionSource);
    Thread t2 = new Thread(bidSource);
    Thread t3 = new Thread(personSource);

    t1.start();
    t2.start();
    t3.start();

    final var generator = new NetworkGenerator(eventsPerSecond,
            timeInSeconds, this.personFraction, this.auctionFraction, this.bidFraction, engineIp,
            auctionNetworkSocketPort, bidNetworkSocketPort, personNetworkSocketPort);

    log.info("Running with {} EPS, {}s max delay for {}s.", eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    long startTime = System.nanoTime();

    var q1Sink = new BenchmarkingSink<Tuple>();
    var q1 = Queries.makeQuery1(bidSource, q1Sink);

    var q2Sink = new BenchmarkingSink<Tuple>();
    var q2 = Queries.makeQueryAgeFilter(personSource, q2Sink);

    var q3Sink = new BenchmarkingSink<Tuple>();
    var q3 = Queries.makeSimpleAuctionQuery(auctionSource, q3Sink);


    var querySinks = List.of(q1Sink, q2Sink, q3Sink);
    var jobManager = new JobManager();
    var done = generator.generate();

    jobManager.addQuery(q1);
    jobManager.addQuery(q2);
    jobManager.addQuery(q3);
    jobManager.runEngine();

    try {
      done.get();
      long endTime = System.nanoTime();
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    jobManager.shutdown();
    querySinks.forEach(BenchmarkingSink::log);
    log.info("Telling Threads to stop");
    auctionSource.stop();
    personSource.stop();
    bidSource.stop();

  }

  public void runBasicBenchmark() {
    log.info("Running with {} EPS, {}s max delay for {}s.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final IntegerTupleGenerator generator1 = new IntegerTupleGenerator(
        eventsPerSecond,
        timeInSeconds,
        executor,
        1
    );
    final IntegerTupleGenerator generator2 = new IntegerTupleGenerator(
        eventsPerSecond,
        timeInSeconds,
        executor,
        2
    );
    log.printf(Level.INFO, "Expecting %,d join tupel",
        generator1.expectedJoinSize(generator2, eventsPerSecond) * timeInSeconds / 5);

    var bs11 = new BlockingSource<Tuple1<Integer>>(maxDelayInSeconds * eventsPerSecond);
    var bs12 = new BlockingSource<Tuple1<Integer>>(maxDelayInSeconds * eventsPerSecond);
    var q0Sink = new BenchmarkingSink<Tuple>();
    var q0 = Queries.makeQuery0Measured(bs11, q0Sink);
    var q1Sink = new BenchmarkingSink<Tuple>();
    var q1 = Queries.makePlainJoin0Measured(bs11, bs12, q1Sink);
    var jobManager = new JobManager();

    var querySinks = new ArrayList<>(List.of(q0Sink, q1Sink));

    jobManager.addQuery(q0);
    jobManager.addQuery(q1);
    jobManager.runEngine();
    var done = generator1.generate(bs11);
    var done2 = generator2.generate(bs12);

    var addedQueries = new ArrayDeque<Query>();
    final long startTime = System.nanoTime();

    try {
      for (int i = 0; i < 59; i++) {
        Thread.sleep(1000);

        var qs = new BenchmarkingSink<Tuple>();
        var q = Queries.makeQuery0Measured(bs11, qs);

        if (i % 5 == 0) {
          qs = new BenchmarkingSink<Tuple>("JoinSink" + i);
          q = Queries.makePlainJoin0Measured(bs11, bs12, qs);
        }
        jobManager.addQuery(q);
        addedQueries.add(q);
        querySinks.add(qs);

        if (i % 4 == 0) {
          var rq = addedQueries.pop();
          jobManager.deleteQuery(rq);
        }
      }
      done.get();
      done2.get();

      long endTime = System.nanoTime();
      log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
      Thread.sleep(TimeUnit.SECONDS.toMillis(maxDelayInSeconds));

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    jobManager.shutdown();
    querySinks.forEach(BenchmarkingSink::log);

  }

  public void runIntBenchmark() {
    long startTime = System.nanoTime();
    log.info("Running with {} EPS, {}s max delay for {}s.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
    final ExecutorService executor = Executors.newFixedThreadPool(this.nThreads);
    final Generator<Tuple1<String>> generator = new StringTupleGenerator(
        eventsPerSecond,
        timeInSeconds,
        executor
    );

    final Random rand = new Random();

    final BlockingSource<Tuple1<String>> source = new BlockingSource<>(
        eventsPerSecond * maxDelayInSeconds);
    final BenchmarkingSink<Tuple2<Integer, Integer>> sink = new BenchmarkingSink<>();

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<Tuple2<Integer, Integer>> streamA = builder.streamOf(source)
        .map(a -> new Tuple2<>(1, 2));
    //.map(a -> new Tuple4<>(a.v1.length(), a.v1.length(), a.v1.length(), a.v1.length()));
    //.filter(e -> true);
    //.map(e -> new Tuple4<>(e.v1.charAt(0), e.v1.charAt(5), e.v1.charAt(10), e.v1.charAt(15)))
    //.map(e -> new Tuple4<>(Integer.valueOf(e.v1), Integer.valueOf(e.v1), Integer.valueOf(e.v1), Integer.valueOf(e.v1)));

    final AStream<Tuple1<Integer>> streamB = builder.streamOf(source)
        .map(e -> new Tuple1<>(e.v1.charAt(rand.nextInt(e.v1.length() - 1))))
        .map(e -> new Tuple1<>(((int) e.v1) % 10))
        .filter(e -> e.v1 != 5)
        .window(new TumblingEventTimeWindow(Time.seconds(10).getNanos()))
        .join(streamA, (a, b) -> new Tuple1<>(a.v1 + b.v1), a -> a.v1, b -> b.v1,
            WatermarkGenerator.seconds(5, 10_000),
            TimestampExtractor.currentTimeNS());

    Query query = new Query(streamA.to(sink).build());

    var engine = new Engine();
    engine.addQuery(query);
    final Benchmark<Tuple1<String>, Tuple2<Integer, Integer>> benchmark = new Benchmark<>(generator,
        engine);
    benchmark.run(source, sink);
    long endTime = System.nanoTime();
    log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
  }

  private Boolean isValidCreditCard(String ccNumber) {
    String pattern = "([0-9]{4}-?){4}";
    Pattern rgxPattern = Pattern.compile(pattern);
    Matcher matcher = rgxPattern.matcher(ccNumber);
    boolean isValid = matcher.find();
    System.out.println(isValid);
    return true;
  }
}
