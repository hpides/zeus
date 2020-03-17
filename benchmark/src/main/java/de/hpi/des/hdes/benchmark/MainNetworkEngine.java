package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.Query;
import java.io.File;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainNetworkEngine implements Runnable {

  @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "1000000")
  private int eventsPerSecond;
  @Option(names = {"--maxDelayInSeconds", "-mds"}, defaultValue = "20")
  private int maxDelayInSeconds;
  @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "120")
  private int timeInSeconds;
  @Option(names = {"--threads", "-t"}, defaultValue = "8")
  private int nThreads;
  @Option(names = {"--auctionSourcePort", "-asp"}, defaultValue = "5551")
  private int auctionNetworkSocketPort;
  @Option(names = {"--bidSourcePort", "-bsp"}, defaultValue = "5552")
  private int bidNetworkSocketPort;
  @Option(names = {"--personSourcePort", "-psp"}, defaultValue = "5553")
  private int personNetworkSocketPort;
  @Option(names = {"--basicPort1", "-bsp1"}, defaultValue = "7001")
  private int basicPort1;
  @Option(names = {"--basicPort2", "-bsp2"}, defaultValue = "7002")
  private int basicPort2;
  @Option(names = {"--serializer", "-seri"}, defaultValue = "custom")
  private String serializer;
  @Option(names = {"--generatorHost", "-gh"}, defaultValue = "172.22.17.248")
  private String generatorHost;
  @Option(names = {"--newQueriesPerSecond", "-nqs"}, defaultValue = "5")
  private int newQueriesPerSecond;
  @Option(names = {"--removeQueriesPerSecond", "-rqs"}, defaultValue = "1")
  private int removeQueriesPerSecond;
  @Option(names = {"--addQueries", "-adq"}, defaultValue = "true")
  private boolean addQueries;

  private int exitAfter = 0;

  public static void main(final String[] args) {
    new CommandLine(new MainNetworkEngine()).execute(args);
  }

  @Override
  public void run() {
    this.exitAfter = this.timeInSeconds + 5;
    basicBenchmarkJoin();
  }

  private AbstractSerializer getSerializer(String dataType) {
    if (dataType.equals("basic")) {
      AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = null;
      if (serializer.equals("gson")) {
        serializerInstance = GSONSerializer.forIntTuple();
      } else if (serializer.equals("custom")) {
        serializerInstance = new IntTupleSerializer();
      } else if (serializer.equals("protobuf")) {
        serializerInstance = null;
      } else {
        log.error("No serializer found with the given name. Exiting");
      }
      return serializerInstance;
    }
    return null;
  }

  private void addQueries(JobManager jobManager, NetworkSource source) {
    var addedQueries = new ArrayDeque<Query>();
    try {
      int totalQueriesToAdd = newQueriesPerSecond * timeInSeconds;
      for (int i = 0; i < totalQueriesToAdd; i += newQueriesPerSecond) {
        Thread.sleep(1000);
        for (int j = 0; j < newQueriesPerSecond; j++) {
          var qs = new BenchmarkingSink<Tuple>();
          var q = Queries.makeQuery0Measured(source, qs);
          jobManager.addQuery(q);
          addedQueries.add(q);
          //querySinks.add(qs);
        }
        for (int k = 0; k < removeQueriesPerSecond; k++) {
          var rq = addedQueries.pop();
          jobManager.deleteQuery(rq);
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void basicBenchmark() {
    log.info("{} {}", basicPort1, basicPort2);

    AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

    final var s1 = new NetworkSource<>(100_000,
        basicPort1, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    final var s2 = new NetworkSource<>(100_000,
        basicPort2, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    new Thread(s1).start();
    new Thread(s2).start();

    String filePathS1 = System.getProperty("user.home") + File.separator + "sink1.csv";
    String filePathS2 = System.getProperty("user.home") + File.separator + "sink2.csv";

//    var q0Sink = new FileSink<Tuple>(filePathS1);
//    var q0 = Queries.makeQuery0Measured(s1, q0Sink);
//    var q1Sink = new FileSink<Tuple>(filePathS2);
//    var q1 = Queries.makeQuery0Measured(s2, q1Sink);
    var q3Sink = new FileSink<Tuple>(filePathS2);
    var q2 = Queries.makePlainJoin0Measured(s1, s2, q3Sink);
    var jobManager = new JobManager();

//    jobManager.addQuery(q0);
//    jobManager.addQuery(q1);
    jobManager.addQuery(q2);

    final long startTime = System.nanoTime();
    jobManager.runEngine();

    if (addQueries) {
      addQueries(jobManager, s1);
    } else {
      try {
        log.info("Waiting for next {} seconds", exitAfter);
        Thread.sleep(TimeUnit.SECONDS.toMillis(exitAfter));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    log.info("stoping after {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    jobManager.shutdown();
    s1.stop();
    s2.stop();
    //querySinks.forEach(BenchmarkingSink::log);
  }

  private void basicBenchmarkJoin() {
    log.info("{} {}", basicPort1, basicPort2);

    AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

    final var s1 = new NetworkSource<>(100_000,
        basicPort1, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    final var s2 = new NetworkSource<>(100_000,
        basicPort2, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    new Thread(s1).start();
    new Thread(s2).start();

    String filePathS1 = System.getProperty("user.home") + File.separator + "sink1.csv";

    var q1Sink = new FileSink<Tuple>(filePathS1);
    var q1 = Queries.makePlainJoin0Measured(s1, s2, q1Sink);
    var jobManager = new JobManager();

    jobManager.addQuery(q1);

    final long startTime = System.nanoTime();
    jobManager.runEngine();

    try {
      log.info("Waiting for next {} seconds", exitAfter);
      Thread.sleep(TimeUnit.SECONDS.toMillis(exitAfter));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    log.info("stoping after {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    jobManager.shutdown();
    s1.stop();
    s2.stop();
  }

  private void basicBenchmarkSingleFilter() {
    log.info("{} {}", basicPort1, basicPort2);

    AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

    final var s1 = new NetworkSource<>(100_000,
        basicPort1, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));

    new Thread(s1).start();

    String filePathS1 = System.getProperty("user.home") + File.separator + "sink1.csv";

    var q0Sink = new FileSink<Tuple>(filePathS1);
    var q0 = Queries.makeQuery0Measured(s1, q0Sink);

    var jobManager = new JobManager();

    jobManager.addQuery(q0);

    final long startTime = System.nanoTime();
    jobManager.runEngine();

    try {
      log.info("Waiting for next {} seconds", exitAfter);
      Thread.sleep(TimeUnit.SECONDS.toMillis(exitAfter));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    log.info("stoping after {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));
    jobManager.shutdown();
    s1.stop();
    //querySinks.forEach(BenchmarkingSink::log);
  }

  private void basicBenchmarkAddQueryFilter() {
    log.info("{} {}", basicPort1, basicPort2);

    AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

    final var s1 = new NetworkSource<>(100_000,
        basicPort1, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));

    new Thread(s1).start();

    String filePathS1 = System.getProperty("user.home") + File.separator + "sink1.csv";

    var q0Sink = new FileSink<Tuple>(filePathS1);
    var q0 = Queries.makeQuery0Measured(s1, q0Sink);

    var jobManager = new JobManager();

    jobManager.addQuery(q0);

    final long startTime = System.nanoTime();
    jobManager.runEngine();

    this.addQueries(jobManager, s1);
    log.info("stoping after {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    jobManager.shutdown();
    s1.stop();
    //querySinks.forEach(BenchmarkingSink::log);
  }


  private void nexmark() {
    // Be aware that the sources are not flushed and could therefore still have events in their buffer when execution is finished
    // The engine will exit after 60 Seconds

    AbstractSerializer<Auction> serializerInstanceAuction = null;
    AbstractSerializer<Bid> serializerInstanceBid = null;
    AbstractSerializer<Person> serializerInstancePerson = null;

    if (serializer.equals("gson")) {
      serializerInstanceAuction = GSONSerializer.forAuction();
      serializerInstanceBid = GSONSerializer.forBid();
      serializerInstancePerson = GSONSerializer.forPerson();
    } else if (serializer.equals("custom")) {
      log.info("Custom serializer is not supported for Nexmark. Exiting");
      System.exit(-1);
    } else if (serializer.equals("protobuf")) {
      log.info("Using Protobuf");
    } else {
      log.error("No serializer found with the given name. Exiting");
      System.exit(-1);
    }

    final var auctionSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
        auctionNetworkSocketPort, serializerInstanceAuction, generatorHost, this.serializer,
        Auction.class,
        Auction::getEventTime);
    final var bidSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
        bidNetworkSocketPort, serializerInstanceBid, generatorHost, this.serializer, Bid.class,
        Bid::getEventTime);
    final var personSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
        personNetworkSocketPort, serializerInstancePerson, generatorHost, this.serializer,
        Person.class,
        Person::getEventTime);

    Thread t1 = new Thread(auctionSource);
    Thread t2 = new Thread(bidSource);
    Thread t3 = new Thread(personSource);

    t1.start();
    t2.start();
    t3.start();

    log.info("Running with {} EPS, {}s max delay for {}s.", eventsPerSecond, maxDelayInSeconds,
        timeInSeconds);

    var q1Sink = new BenchmarkingSink<Tuple>();
    var q1 = Queries.makeQuery1(bidSource, q1Sink);

    var q2Sink = new BenchmarkingSink<Tuple>();
    var q2 = Queries.makeQueryAgeFilter(personSource, q2Sink);

    var q3Sink = new BenchmarkingSink<Tuple>();
    var q3 = Queries.makeSimpleAuctionQuery(auctionSource, q3Sink);

    var querySinks = List.of(q1Sink, q2Sink, q3Sink);
    var jobManager = new JobManager();

    jobManager.addQuery(q1);
    jobManager.addQuery(q2);
    jobManager.addQuery(q3);
    jobManager.runEngine();

    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(exitAfter));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    jobManager.shutdown();
    querySinks.forEach(BenchmarkingSink::log);
    log.info("Telling Threads to stop");

    auctionSource.stop();
    personSource.stop();
    bidSource.stop();
  }
}
