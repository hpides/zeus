package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple4;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainNetworkEngine implements Runnable {

  @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "120")
  private int timeInSeconds;
  @Option(names = {"--auctionSourcePort", "-asp"}, defaultValue = "5551")
  private int auctionNetworkSocketPort;
  @Option(names = {"--bidSourcePort", "-bsp"}, defaultValue = "5552")
  private int bidNetworkSocketPort;
  @Option(names = {"--basicPort1", "-bsp1"}, defaultValue = "7001")
  private int basicPort1;
  @Option(names = {"--basicPort2", "-bsp2"}, defaultValue = "7002")
  private int basicPort2;
  @Option(names = {"--serializer", "-seri"}, defaultValue = "custom")
  private String serializer;
  @Option(names = {"--generatorHost", "-gh"}, defaultValue = "172.0.0.1")
  private String generatorHost;
  @Option(names = {"--newQueriesPerSecond", "-nqs"}, defaultValue = "0")
  private int newQueriesPerBatch;
  @Option(names = {"--removeQueriesPerSecond", "-rqs"}, defaultValue = "0")
  private int removeQueriesPerBatch;
  @Option(names = {"--waitBetweenAddDel", "-wbt"}, defaultValue = "0")
  private int waitSecondsBetweenBatches;
  @Option(names = {"--fixedQueries", "-fq"}, defaultValue = "1")
  private int fixedQueries;
  @Option(names = {"--type", "-t"}, defaultValue = "basic")
  private String benchmarkType;
  @Option(names = {"--operator", "-op"}, defaultValue = "map")
  private String operator;

  private int exitAfter = 0;
  private long startTime;

  public static void main(final String[] args) {
    StringBuilder params = new StringBuilder();
    for (String s : args) {
      params.append(s).append(" ");
    }
    log.info(params.toString());
    new CommandLine(new MainNetworkEngine()).execute(args);
  }

  @Override
  public void run() {
    log.info("Connection to {}", this.generatorHost);
    this.exitAfter = this.timeInSeconds + 1;
    if (this.benchmarkType.equals("basic")) {
      log.info("Running with basic data for {} seconds. With type {} and op {}", this.exitAfter,
          this.benchmarkType, this.operator);
      if (this.operator.equals("map")) {
        basicAddDeleteMap();
      }
      if (this.operator.equals("join")) {
        basicAddDeleteJoin();
      }
      if (this.operator.equals("ajoin")) {
        basicAddDeleteAJoin();
      }
    } else {
      log.info("Running with nexmark data for {} seconds", this.exitAfter);
      if (this.operator.equals("1")) {
        this.nexmarkLightAddRemoveQueryFilter();
      }
      if (this.operator.equals("2")) {
        this.nexmarkLightAddRemoveQueryPlainJoin();
      }
      if (this.operator.equals("3")) {
        this.nexmarkLightAddRemoveQueryAJoin();
      }
      if (this.operator.equals("4")) {
        this.nexmarkLightHottestCategory();
      }
      if (this.operator.equals("5")) {
        this.nexmarkLightHighestPricePerAuction();
      }
    }
  }

  private void basicAddDeleteMap() {
    var s1 = this.prepareIntSources(basicPort1);
    var jobManager = new JobManager();
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeQuery0Measured(s1, sink), "map");
    addAndDeleteQueries(jobManager, (Sink<Tuple> sink) -> Queries.makeQuery0Measured(s1, sink), qs);

    waitForFinish(List.of(s1), jobManager);
  }

  private void basicAddDeleteJoin() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareIntSources(basicPort1);
    var s2 = this.prepareIntSources(basicPort2);

    var jobManager = new JobManager();
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makePlainJoin0Measured(s1, s2, sink),
        "join");
    addAndDeleteQueries(jobManager, (sink) -> Queries.makePlainJoin0Measured(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager);
  }

  private void basicAddDeleteAJoin() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareIntSources(basicPort1);
    var s2 = this.prepareIntSources(basicPort2);

    var jobManager = new JobManager();
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeAJoin0Measured(s1, s2, sink),
        "ajoin");
    addAndDeleteQueries(jobManager, (sink) -> Queries.makeAJoin0Measured(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager);
  }


  private String formatName(int id, String name) {
    return id + "_" + name + "_a%" + this.newQueriesPerBatch + "%_" + "r%"
        + this.removeQueriesPerBatch + "%_e%" + this.waitSecondsBetweenBatches + "%" + "_f%"
        + this.fixedQueries + "%";
  }


  // ----- Nexmark Add Remove Query -----
  private void nexmarkLightAddRemoveQueryFilter() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareAuctionSource(basicPort2);
    var jobManager = new JobManager();
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeNexmarkLightFilterMeasured(s1, sink),
        "NARFilter");

    addAndDeleteQueries(jobManager,
        (Sink<Tuple> sink) -> Queries.makeNexmarkLightFilterMeasured(s1, sink), qs);

    waitForFinish(List.of(s1), jobManager);
  }

  private void nexmarkLightAddRemoveQueryPlainJoin() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);

    var jobManager = new JobManager();

    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeNexmarkLightPlainJoinMeasured(s1, s2,
        sink),
        "NARpJoin");

    addAndDeleteQueries(jobManager,
        (Sink<Tuple> sink) -> Queries.makeNexmarkLightPlainJoinMeasured(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager);
  }

  private void nexmarkLightAddRemoveQueryAJoin() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);

    var jobManager = new JobManager();

    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeNexmarkLightAJoinMeasured(s1, s2,
        sink),
        "NARaJoin");

    addAndDeleteQueries(jobManager,
        (Sink<Tuple> sink) -> Queries.makeNexmarkLightAJoinMeasured(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager);
  }

  // ----- Nextmark Semenatic Queries -----
  private void nexmarkLightHottestCategory() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);

    var q1Sink = new FileSink<Tuple>(formatName(1, "NHotCat"), 1);
    var q1 = Queries.makeNexmarkHottestCategory(s1, s2, q1Sink);
    var jobManager = new JobManager();
    jobManager.addQuery(q1);
    this.fixedQueries--;
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeNexmarkHottestCategory(s1, s2, sink),
        "NHotCat", 0, 1);
    addAndDeleteQueries(jobManager,
        (Sink<Tuple> sink) -> Queries.makeNexmarkHottestCategory(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager, q1Sink);
  }

  private void nexmarkLightHighestPricePerAuction() {
    log.info("{} {}", basicPort1, basicPort2);

    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);

    var q1Sink = new FileSink<Tuple>(formatName(1, "NHighPPA"), 1);
    var q1 = Queries.makeNexmarkMaxiumPriceForAuction(s1, s2, q1Sink);
    var jobManager = new JobManager();
    jobManager.addQuery(q1);
    this.fixedQueries--;
    var qs = addFixedQueries(jobManager, (sink) -> Queries.makeNexmarkMaxiumPriceForAuction(s1, s2, sink),
        "NHighPPA", 0, 1);
    addAndDeleteQueries(jobManager,
        (Sink<Tuple> sink) -> Queries.makeNexmarkMaxiumPriceForAuction(s1, s2, sink), qs);

    waitForFinish(List.of(s1, s2), jobManager, q1Sink);
  }

  private void waitForFinish(List<NetworkSource<?>> sourceList, JobManager jobManager) {
    waitForFinish(sourceList, jobManager, null);
  }

  private void waitForFinish(List<NetworkSource<?>> sourceList, JobManager jobManager, FileSink<?> sinkToFlush) {
    jobManager.runEngine();
    final long currentTime = System.nanoTime();
    long wait = this.startTime + TimeUnit.SECONDS.toNanos(this.exitAfter) - currentTime;
    if (wait > 0) {
      try {
        log.info("Waiting for next {} seconds", TimeUnit.NANOSECONDS.toSeconds(wait));
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(wait));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    log.info("stoping after {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    jobManager.shutdown();
    sourceList.forEach(NetworkSource::stop);
    if(sinkToFlush != null) {
      sinkToFlush.flush();
    }
    System.exit(0);
  }

  private ArrayDeque<Query> addFixedQueries(JobManager jobManager,
      Function<Sink<Tuple>, Query> makeQuery, String prefix, int nrWriting, int writeEveryXTuple) {
    var queries = new ArrayDeque<Query>();
    for (int i = 1; i <= this.fixedQueries; i++) {
      if (i <= nrWriting) {
        var qSink = new FileSink<Tuple>(formatName(i, prefix), writeEveryXTuple);
        var q1 = makeQuery.apply(qSink);
        jobManager.addQuery(q1);
      } else {
        var qSink = new NoOpSink<Tuple>();
        var q1 = makeQuery.apply(qSink);
        jobManager.addQuery(q1);
        queries.add(q1);
      }
    }

    jobManager.runEngine();
    return queries;
  }

  private ArrayDeque<Query> addFixedQueries(JobManager jobManager,
      Function<Sink<Tuple>, Query> makeQuery, String prefix) {

    return this.addFixedQueries(jobManager, makeQuery, prefix, 1, 1000);
  }


  private void addAndDeleteQueries(JobManager jobManager,
      Function<Sink<Tuple>, Query> createQuery, ArrayDeque<Query> addedQueries) {
    jobManager.runEngine();
    try {
      if (waitSecondsBetweenBatches < 1) {
        log.warn("Wait between seconds must be >= 1");
        return;
      }
      int batches = (timeInSeconds / waitSecondsBetweenBatches);
      for (int i = 0; i < batches; i++) {
        for (int j = 0; j < newQueriesPerBatch; j++) {
          var qs = new NoOpSink<Tuple>();
          Query q = createQuery.apply(qs);
          jobManager.addQuery(q);
          addedQueries.add(q);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(waitSecondsBetweenBatches));
        for (int k = 0; k < removeQueriesPerBatch; k++) {

          if (addedQueries.size() > 0) {
            var rq = addedQueries.pop();
            jobManager.deleteQuery(rq);
          } else {
            log.warn("No query left to remove");
          }
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private AbstractSerializer getSerializer(String dataType) {
    if (dataType.equals("basic")) {
      AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = null;
      if (serializer.equals("gson")) {
        serializerInstance = GSONSerializer.forIntTuple();
      } else if (serializer.equals("custom")) {
        serializerInstance = new IntTupleSerializer();
      } else {
        log.error("No serializer found with the given name. Exiting");
      }
      return serializerInstance;
    }
    return null;
  }

  private NetworkSource<Tuple2<Integer, Long>> prepareIntSources(int port) {
    AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");
    NetworkSource<Tuple2<Integer, Long>> s1 = new NetworkSource<>(10_000,
        port, serializerInstance, generatorHost, this.serializer, null,
        t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    new Thread(s1).start();
    this.startTime = System.nanoTime();
    return s1;
  }

  private NetworkSource<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> prepareBidSource(
      int port) {
    var serializerInstance = new NexmarkLightBidDeSerializer();

    var s1 = new NetworkSource<>(10_000,
        port, serializerInstance, generatorHost, this.serializer, null,
        t5 -> TimeUnit.MILLISECONDS.toNanos(t5.v2));
    new Thread(s1).start();
    this.startTime = System.nanoTime();
    return s1;
  }

  private NetworkSource<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> prepareAuctionSource(
      int port) {
    var serializerInstance = new NexmarkLightAuctionDeSerializer();

    var s1 = new NetworkSource<>(10_000,
        port, serializerInstance, generatorHost, this.serializer, null,
        t5 -> TimeUnit.MILLISECONDS.toNanos(t5.v2));
    new Thread(s1).start();
    this.startTime = System.nanoTime();
    return s1;
  }
}
