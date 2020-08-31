package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.CompiledEngine;
import de.hpi.des.hdes.engine.JobManager;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.VulcanoEngine;
import de.hpi.des.hdes.engine.cstream.CStream;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.window.CWindow;
import de.hpi.des.hdes.engine.window.Time;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class MainNetworkEngine implements Runnable {

    private static final List<String> types = List.of("bjoin", "bajoin", "compiledajoin", "compiledjoin", "compiledagg",
            "report_ajoin", "report_join", "report_shared_ajoin", "report_parallel_ajoin");

    // CLI Options
    @Option(names = { "--timeInSeconds", "-tis" }, defaultValue = "100")
    private int timeInSeconds;
    @Option(names = { "--basicPort1", "-bsp1" }, defaultValue = "5551")
    private int basicPort1;
    @Option(names = { "--basicPort2", "-bsp2" }, defaultValue = "5552")
    private int basicPort2;
    @Option(names = { "--serializer", "-seri" }, defaultValue = "custom")
    private String serializer;
    @Option(names = { "--generatorHost", "-gh" }, defaultValue = "172.0.0.1")
    private String generatorHost;
    @Option(names = { "--numberSharedQueries", "-nsq" }, defaultValue = "1")
    private int numberSharedQueries;
    @Option(names = { "--type", "-t" }, defaultValue = "report_join")
    private String benchmarkType;
    @Option(names = { "--networkBufferSize", "-nbs" }, defaultValue = "1000")
    private int bufferinK;
    @Option(names = { "--packageRoot" }, defaultValue = "")
    private String packageRoot;
    @Option(names = { "--outputPath" }, defaultValue = "")
    private String outputPath;
    @Option(names = { "--logging" }, defaultValue = "false")
    private boolean logging;

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
        if (timeInSeconds < 1) {
            timeInSeconds = 1;
        }
        if (logging) {
            Dispatcher.LOGGING_ENABLED(true);
        }
        if (!packageRoot.equals("")) {
            DirectoryHelper.setPackageRoot(packageRoot);
        }
        if (!outputPath.equals("")) {
            DirectoryHelper.setOutputPath(outputPath);
        }
        switch (benchmarkType) {
            case "report_ajoin": {
                reportBenchmarkAJoin();
                break;
            }
            case "report_join": {
                reportBenchmarkJoin();
                break;
            }
            case "report_shared_ajoin": {
                reportSharedAJoin();
                break;
            }
            case "report_parallel_ajoin": {
                reportParallelAJoin();
                break;
            }
            case "bjoin": {
                basicAddDeleteJoin();
                break;
            }
            case "bajoin": {
                basicAddDeleteAJoin();
                break;
            }
            case "compiledajoin": {
                executeCompiledAJoin();
                break;
            }
            case "compiledjoin": {
                executeCompiledJoin();
                break;
            }
            case "compiledagg": {
                executeCompiledAggregation();
                break;
            }
            default:
                log.warn("There was an error with benchmark {}", benchmarkType);
        }
        System.exit(0);
    }

    private void reportParallelAJoin() {
        JobManager manager = new JobManager(new CompiledEngine());

        for (int i = 0; i < numberSharedQueries; i++) {
            VulcanoTopologyBuilder tempBuilder = new VulcanoTopologyBuilder();
            CStream tempSource = tempBuilder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost,
                    (basicPort1 + i * 2));
            tempBuilder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, (basicPort2 + i * 2))
                    .ajoin(tempSource, new PrimitiveType[] { PrimitiveType.INT },
                            new PrimitiveType[] { PrimitiveType.INT }, 0, 0, CWindow.tumblingWindow(Time.seconds(5)))
                    .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                            new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                    "(_,_) -> System.currentTimeMillis()"))
                    .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);
            Query tempQuery = tempBuilder.buildAsQuery();

            manager.addQuery(tempQuery);
            log.info("Added query number {}", i + 1);
        }

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        manager.shutdown();
    }

    private void reportBenchmarkJoin() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

        CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .join(sourceOne, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(5)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                "(_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);

        manager.addQuery(builder.buildAsQuery());
        // running engine

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        manager.shutdown();
    }

    private void reportBenchmarkAJoin() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        // Use tumbling windows (5 seconds)

        CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .ajoin(sourceOne, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(5)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                "(_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);
        Query q1 = builder.buildAsQuery();

        manager.addQuery(q1);
        // Running engine

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        manager.shutdown();
    }

    private void basicAddDeleteJoin() {
        var s1 = this.prepareIntSources(basicPort1);
        var s2 = this.prepareIntSources(basicPort2);
        executeQuery((sink) -> Queries.makePlainJoin0Measured(s1, s2, sink),
                new FileSinkFactory("basic_join", 1, 0, 0, 0, 10000), List.of(s1, s2));
    }

    private void basicAddDeleteAJoin() {
        var s1 = this.prepareIntSources(basicPort1);
        var s2 = this.prepareIntSources(basicPort2);
        executeQuery((sink) -> Queries.makeAJoin0Measured(s1, s2, sink),
                new FileSinkFactory("basic_ajoin", 1, 0, 0, 0, 10000), List.of(s1, s2));
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

    private void executeQuery(Function<Sink<Tuple>, Query> makeQuery, FileSinkFactory factory,
            List<NetworkSource> sources) {
        executeQuery(makeQuery, factory, sources, 1);
    }

    private void reportSharedAJoin() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        // Use tumbling windows (5 seconds)

        CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .ajoin(sourceOne, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(5)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                "(_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);
        Query q1 = builder.buildAsQuery();

        manager.addQuery(q1);
        // Running engine

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        for (int i = 1; i < numberSharedQueries; i++) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            VulcanoTopologyBuilder tempBuilder = new VulcanoTopologyBuilder();
            CStream tempSource = tempBuilder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost,
                    basicPort1);
            tempBuilder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                    .ajoin(tempSource, new PrimitiveType[] { PrimitiveType.INT },
                            new PrimitiveType[] { PrimitiveType.INT }, 0, 0, CWindow.tumblingWindow(Time.seconds(5)))
                    .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                            new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT })
                                    .add(PrimitiveType.INT, "(_,_) -> " + i)
                                    .add(PrimitiveType.LONG, "(_,_,_) -> System.currentTimeMillis()"))
                    .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
                            PrimitiveType.LONG }, 10000);
            Query tempQuery = tempBuilder.buildAsQuery();

            manager.addQuery(tempQuery);
            log.info("Added query number {}", i + 1);
        }

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds - numberSharedQueries * 10 - 15));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        manager.shutdown();
    }

    private void executeCompiledAJoin() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        // Use tumbling windows (5 seconds)
        // Present results next week

        CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .ajoin(sourceOne, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(1)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                "(_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);
        Query q1 = builder.buildAsQuery();

        VulcanoTopologyBuilder builder2 = new VulcanoTopologyBuilder();
        CStream sourceOne2 = builder2.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder2.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .ajoin(sourceOne2, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(1)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT })
                                .add(PrimitiveType.LONG, "(_,_) -> System.currentTimeMillis()")
                                .add(PrimitiveType.LONG, "(_,_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG,
                        PrimitiveType.LONG }, 10000);
        Query q2 = builder2.buildAsQuery();

        VulcanoTopologyBuilder builder3 = new VulcanoTopologyBuilder();
        CStream sourceOne3 = builder3.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder3.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .ajoin(sourceOne3, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(1)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT })
                                .add(PrimitiveType.LONG, "(_,_) -> System.currentTimeMillis()")
                                .add(PrimitiveType.LONG, "(_,_,_) -> System.currentTimeMillis()")
                                .add(PrimitiveType.LONG, "(_,_,_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG,
                        PrimitiveType.LONG, PrimitiveType.LONG }, 10000);
        Query q3 = builder3.buildAsQuery();

        manager.addQuery(q1);
        // Running engine

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        log.info("Shutting down engine");
        manager.shutdown();
    }

    private void executeCompiledJoin() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

        CStream sourceOne = builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort2)
                .join(sourceOne, new PrimitiveType[] { PrimitiveType.INT }, new PrimitiveType[] { PrimitiveType.INT },
                        0, 0, CWindow.tumblingWindow(Time.seconds(1)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }).add(PrimitiveType.LONG,
                                "(_,_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.LONG }, 10000);

        manager.addQuery(builder.buildAsQuery());
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        manager.shutdown();
    }

    private void executeCompiledAggregation() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

        builder.streamOfC(new PrimitiveType[] { PrimitiveType.INT }, generatorHost, basicPort1)
                .count(new PrimitiveType[] { PrimitiveType.INT }, 0, CWindow.tumblingWindow(Time.seconds(1)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(new PrimitiveType[] { PrimitiveType.INT })
                        .add(PrimitiveType.LONG, "(_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.LONG }, 1);

        manager.addQuery(builder.buildAsQuery());

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        manager.shutdown();
    }

    private void compiledNexmarkMaxiumPriceForAuction() {
        JobManager manager = new JobManager(new CompiledEngine());
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

        CStream auctionSource = builder.streamOfC(
                new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
                generatorHost, basicPort1);

        CStream bidSource = builder.streamOfC(
                new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
                generatorHost, basicPort2);

        bidSource.ajoin(auctionSource,
                new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
                new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT }, 1,
                0, CWindow.tumblingWindow(Time.seconds(5)))
                .filter(new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT,
                        PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT,
                        PrimitiveType.INT }, "(_,_,_,v1,_,_,_,v2) -> v1 > v2")
                .maximum(new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT,
                        PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT,
                        PrimitiveType.INT }, 3, CWindow.tumblingWindow(Time.seconds(5)))
                .map(new de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple(new PrimitiveType[] { PrimitiveType.INT })
                        .add(PrimitiveType.LONG, "(_) -> System.currentTimeMillis()"))
                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.LONG }, 1);

        manager.addQuery(builder.buildAsQuery());
        // Running engine
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        manager.shutdown();
    }

    private void executeQuery(Function<Sink<Tuple>, Query> makeQuery, FileSinkFactory factory,
            List<NetworkSource> sources, int fileSinksAmount) {
        try {
            // Query Array
            ArrayDeque<Query> queries = new ArrayDeque<>();
            // Creating a JobManager
            VulcanoEngine engine = new VulcanoEngine();
            JobManager manager = new JobManager(engine);
            // Adding fixed queries
            Sink sink;
            // Create Sink
            if (fileSinksAmount > factory.getSinkAmount()) {
                sink = factory.createFileSink(0);
            } else {
                sink = new NoOpSink();
            }
            Query query = makeQuery.apply(sink);
            manager.addQuery(query);
            queries.add(query);
            // Running engine
            engine.run();
            Thread.sleep(TimeUnit.SECONDS.toMillis(timeInSeconds));

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
