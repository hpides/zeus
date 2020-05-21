package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.VulcanoEngine;
import de.hpi.des.hdes.engine.operation.Sink;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple4;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class MainNetworkEngine implements Runnable {

  private static final List<String> types = List.of("bmap", "bjoin", "bajoin", "nfilter", "njoin", "najoin",
      "najoin_aggregated", "hotcat", "maxpric", "filter", "ifilter", "gfilter");

  // CLI Options
  @Option(names = { "--timeInSeconds", "-tis" }, defaultValue = "120")
  private int timeInSeconds;
  @Option(names = { "--auctionSourcePort", "-asp" }, defaultValue = "5551")
  private int auctionNetworkSocketPort;
  @Option(names = { "--bidSourcePort", "-bsp" }, defaultValue = "5552")
  private int bidNetworkSocketPort;
  @Option(names = { "--basicPort1", "-bsp1" }, defaultValue = "7001")
  private int basicPort1;
  @Option(names = { "--basicPort2", "-bsp2" }, defaultValue = "7002")
  private int basicPort2;
  @Option(names = { "--serializer", "-seri" }, defaultValue = "custom")
  private String serializer;
  @Option(names = { "--generatorHost", "-gh" }, defaultValue = "172.0.0.1")
  private String generatorHost;
  @Option(names = { "--newQueriesPerBatch", "-nqs" }, defaultValue = "0")
  private int newQueriesPerBatch;
  @Option(names = { "--removeQueriesPerBatch", "-rqs" }, defaultValue = "0")
  private int removeQueriesPerBatch;
  @Option(names = { "--batchesAmount", "-bat" }, defaultValue = "0")
  private int batches;
  @Option(names = { "--fixedQueries", "-fq" }, defaultValue = "1")
  private int fixedQueries;
  @Option(names = { "--type", "-t" }, defaultValue = "bmap")
  private String benchmarkType;
  @Option(names = { "--networkBufferSize", "-nbs" }, defaultValue = "10")
  private int bufferinK;

  // Calculated values
  private int waitSecondsBetweenBatches = 0;

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
    // Sanity checking parameters
    if (!types.contains(benchmarkType)) {
      log.error("No such benchmark type {}.", benchmarkType);
      log.info("You may use one of {}.", types.toString());
      System.exit(1);
    }
    if (timeInSeconds < 1)
      timeInSeconds = 1;
    if (batches < 0)
      batches = 0;
    if (fixedQueries < 1)
      fixedQueries = 1;
    log.info("Running Benchmark {} with {} queries for {} seconds with {} batches.", benchmarkType, fixedQueries,
        timeInSeconds, batches);
    log.info("Connection to {}.", generatorHost);
    if (batches > 0) {
      waitSecondsBetweenBatches = (timeInSeconds / batches);
      log.info("Adding {} queries per batch and removing {} queries per batch", newQueriesPerBatch,
          removeQueriesPerBatch);
      if (newQueriesPerBatch < removeQueriesPerBatch) {
        log.error(
            "Please do not remove any of the fixed queries. Choose at least the same amount of newQueries ofr deletedQueries");
        System.exit(1);
      }
    }
    switch (benchmarkType) {
      case "bmap": {
        basicAddDeleteMap();
      }
      case "bjoin": {
        basicAddDeleteJoin();
      }
      case "bajoin": {
        basicAddDeleteAJoin();
      }
      case "nfilter": {
        nexmarkLightAddRemoveQueryFilter();
      }
      case "njoin": {
        nexmarkLightAddRemoveQueryPlainJoin();
      }
      case "najoin": {
        nexmarkLightAddRemoveQueryAJoin();
      }
      case "hotcat": {
        nexmarkLightHottestCategory();
      }
      case "maxpric": {
        nexmarkLightHighestPricePerAuction();
      }
      case "filter": {
        filter();
      }
      default:
        log.warn("There was an error with benchmark {}", benchmarkType);
    }
  }

  private void basicAddDeleteMap() {
    var s1 = this.prepareIntSources(basicPort1);
    executeQuery((sink) -> Queries.makeQuery0Measured(s1, sink),
        new FileSinkFactory("basic_map", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1));
  }

  private void basicAddDeleteJoin() {
    var s1 = this.prepareIntSources(basicPort1);
    var s2 = this.prepareIntSources(basicPort2);
    executeQuery((sink) -> Queries.makePlainJoin0Measured(s1, s2, sink),
        new FileSinkFactory("basic_join", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1, s2));
  }

  private void basicAddDeleteAJoin() {
    var s1 = this.prepareIntSources(basicPort1);
    var s2 = this.prepareIntSources(basicPort2);
    executeQuery((sink) -> Queries.makeAJoin0Measured(s1, s2, sink),
        new FileSinkFactory("basic_ajoin", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1, s2));
  }

  // ----- Nexmark Add Remove Query -----
  private void nexmarkLightAddRemoveQueryFilter() {
    var s1 = this.prepareAuctionSource(basicPort2);
    executeQuery((sink) -> Queries.makeNexmarkLightFilterMeasured(s1, sink),
        new FileSinkFactory("nexmark_filter", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1));
  }

  private void nexmarkLightAddRemoveQueryPlainJoin() {
    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);
    executeQuery((sink) -> Queries.makeNexmarkLightPlainJoinMeasured(s1, s2, sink),
        new FileSinkFactory("nexmark_join", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1, s2));
  }

  private void nexmarkLightAddRemoveQueryAJoin() {
    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);
    executeQuery((sink) -> Queries.makeNexmarkLightAJoinMeasured(s1, s2, sink),
        new FileSinkFactory("nexmark_ajoin", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1000),
        List.of(s1, s2));
  }

  // ----- Nextmark Semenatic Queries -----
  private void nexmarkLightHottestCategory() {
    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);
    executeQuery((sink) -> Queries.makeNexmarkHottestCategory(s1, s2, sink),
        new FileSinkFactory("nexmark_hotcat", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1, s2), 1);
  }

  private void nexmarkLightHighestPricePerAuction() {
    var s1 = this.prepareBidSource(basicPort1);
    var s2 = this.prepareAuctionSource(basicPort2);
    executeQuery((sink) -> Queries.makeNexmarkMaxiumPriceForAuction(s1, s2, sink),
        new FileSinkFactory("nexmark_maxpri", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 1),
        List.of(s1, s2), 1);
  }

  // JNIO Microbenchmark
  private void filter() {
    var source = this.prepareIntSources(basicPort1);
    source.setProfilingEvents(true);
    executeQuery((sink) -> Queries.makeFilter0Measured(source, sink),
        new FileSinkFactory("filter_jvm", fixedQueries, batches, newQueriesPerBatch, removeQueriesPerBatch, 100),
        List.of(source));
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
    NetworkSource<Tuple2<Integer, Long>> s1 = new NetworkSource<>(1000 * bufferinK, port, serializerInstance,
        generatorHost, this.serializer, null, t2 -> TimeUnit.MILLISECONDS.toNanos(t2.v2));
    new Thread(s1).start();
    return s1;
  }

  private NetworkSource<Tuple2<Tuple4<Long, Long, Integer, Integer>, Long>> prepareBidSource(int port) {
    var serializerInstance = new NexmarkLightBidDeSerializer();

    var s1 = new NetworkSource<>(1000 * bufferinK, port, serializerInstance, generatorHost, this.serializer, null,
        t5 -> TimeUnit.MILLISECONDS.toNanos(t5.v2));
    new Thread(s1).start();
    return s1;
  }

  private NetworkSource<Tuple2<Tuple4<Long, Integer, Integer, Integer>, Long>> prepareAuctionSource(int port) {
    var serializerInstance = new NexmarkLightAuctionDeSerializer();

    var s1 = new NetworkSource<>(1000 * bufferinK, port, serializerInstance, generatorHost, this.serializer, null,
        t5 -> TimeUnit.MILLISECONDS.toNanos(t5.v2));
    new Thread(s1).start();
    return s1;
  }

  private void executeQuery(Function<Sink<Tuple>, Query> makeQuery, FileSinkFactory factory,
      List<NetworkSource> sources) {
    executeQuery(makeQuery, factory, sources, fixedQueries);
  }

  private void executeQuery(Function<Sink<Tuple>, Query> makeQuery, FileSinkFactory factory,
      List<NetworkSource> sources, int fileSinksAmount) {
    try {
      // Query Array
      ArrayDeque<Query> queries = new ArrayDeque<>();
      // Creating a JobManager
      JobManager manager = new JobManager(new VulcanoEngine());
      // Adding fixed queries
      for (int i = 0; i < fixedQueries; i++) {
        Sink sink;
        // Create Sink
        if (fileSinksAmount > factory.getSinkAmount()) {
          sink = factory.createFileSink(i);
        } else {
          sink = new NoOpSink();
        }
        Query query = makeQuery.apply(sink);
        manager.addQuery(query);
        queries.add(query);
      }
      // Running engine
      manager.runEngine();
      if (batches == 0) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
      }
      // Adding and removing batches of queries
      for (int i = 0; i < batches; i++) {
        // Adding queries per batch
        for (int j = 0; j < newQueriesPerBatch; j++) {
          NoOpSink noop = new NoOpSink();
          Query q = makeQuery.apply(noop);
          queries.add(q);
          manager.addQuery(q);
        }
        // Waiting for specified time
        Thread.sleep(TimeUnit.SECONDS.toMillis(waitSecondsBetweenBatches));
        // Removing queries per batch
        for (int j = 0; j < removeQueriesPerBatch; j++) {
          Query q = queries.pop();
          manager.deleteQuery(q);
        }
      }
      // Waiting for specified time
      Thread.sleep(TimeUnit.SECONDS.toMillis(waitSecondsBetweenBatches));
      // Shutting down manager
      manager.shutdown();
      // Stopping network sources
      sources.forEach(NetworkSource::stop);
      // Flushing ech sink
      factory.flushSinks();
      System.exit(0);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
