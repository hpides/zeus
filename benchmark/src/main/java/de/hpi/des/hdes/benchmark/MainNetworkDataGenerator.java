package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.ByteGenerator;
import de.hpi.des.hdes.benchmark.generator.IntegerTupleGenerator;
import de.hpi.des.hdes.benchmark.generator.NexmarkByteAuctionGenerator;
import de.hpi.des.hdes.benchmark.generator.NexmarkByteBidGenerator;
import de.hpi.des.hdes.benchmark.generator.NexmarkLightAuctionGenerator;
import de.hpi.des.hdes.benchmark.generator.NexmarkLightBidGenerator;
import de.hpi.des.hdes.benchmark.nexmark.NexmarkLightDataGenerator;
import de.hpi.des.hdes.engine.io.DirectoryHelper;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Log4j2
public class MainNetworkDataGenerator implements Runnable {

    @Option(names = { "--eventsPerSecond", "-eps" }, defaultValue = "10000000")
    private long eventsPerSecond;
    @Option(names = { "--timeInSeconds", "-tis" }, defaultValue = "30")
    private long timeInSeconds;
    @Option(names = { "--auctionSourcePort", "-asp" }, defaultValue = "5551")
    private int auctionNetworkSocketPort;
    @Option(names = { "--bidSourcePort", "-bsp" }, defaultValue = "5552")
    private int bidNetworkSocketPort;
    @Option(names = { "--personSourcePort", "-psp" }, defaultValue = "5553")
    private int personNetworkSocketPort;
    @Option(names = { "--basicPort1", "-bsp1" }, defaultValue = "7001")
    private int basicPort1;
    @Option(names = { "--basicPort2", "-bsp2" }, defaultValue = "7002")
    private int basicPort2;
    @Option(names = { "--serializer", "-seri" }, defaultValue = "custom")
    private String serializer;
    @Option(names = { "--type", "-t" }, defaultValue = "nex")
    private String benchmarkType;
    @Option(names = { "--amountOfSources", "-ams" }, defaultValue = "2")
    private int amountOfSources;
    @Option(names = { "--outputPath" }, defaultValue = "")
    private String outputPath;

    public static void main(final String[] args) {
        for (String s : args) {
            log.info(s);
        }
        new CommandLine(new MainNetworkDataGenerator()).execute(args);
    }

    @Override
    public void run() {
        if (!outputPath.equals("")) {
            DirectoryHelper.setOutputPath(outputPath);
        }
        if (this.benchmarkType.equals("basic")) {
            log.info("Running with basic data");
            if (amountOfSources == 1) {
                basicBenchmarkOneSource();
            } else if (amountOfSources == 2) {
                basicBenchmarkTwoSources();
            } else {
                log.error("THIS AMOUNT OF SOURCES IS NOT VALID; FIX amountOfSources-PARAMETER");
                System.exit(-1);
            }
        } else if (this.benchmarkType.equals("new")) {
            if (amountOfSources == 1) {
                newBenchmarkOneSource();
            } else if (amountOfSources == 2) {
                newBenchmarkTwoSources();
            } else {
                log.error("THIS AMOUNT OF SOURCES IS NOT VALID; FIX amountOfSources-PARAMETER");
                System.exit(-1);
            }
        } else if (this.benchmarkType.equals("nex")) {
            newNexmarkTwoSources();
        } else {
            log.info("Running with nexmark data");
            if (amountOfSources == 1) {
                nexmarkLightOneSource();
            } else if (amountOfSources == 2) {
                nexmarkLightTwoSources();
            } else {
                log.error("THIS AMOUNT OF SOURCES IS NOT VALID; FIX amountOfSources-PARAMETER");
                System.exit(-1);
            }
        }
        System.exit(0);
    }

    private AbstractSerializer getSerializer(String benchmarkType) {
        if (benchmarkType.equals("basic")) {
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

    private void basicBenchmarkOneSource() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final var generator1 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor, 1);

        try {
            AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");
            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File, this.timeInSeconds,
                    (int) eventsPerSecond);
            s1.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            done.get();
            long endTime = System.nanoTime();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
            s1.writeFile();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void newBenchmarkOneSource() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor1 = Executors.newFixedThreadPool(1);
        final var generator1 = new ByteGenerator(eventsPerSecond, timeInSeconds, executor1, 1);
        log.printf(Level.INFO, "Expecting %,d tupel", generator1.expectedAggregationSize(timeInSeconds, 1));

        try {
            AbstractSerializer<byte[]> serializerInstance = new ByteSerializer();

            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File, this.timeInSeconds,
                    (int) eventsPerSecond);
            s1.setByteFlag(true);
            s1.setByteLength(13);
            s1.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            done.get();
            long endTime = System.nanoTime();
            s1.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void newBenchmarkTwoSources() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor1 = Executors.newFixedThreadPool(1);
        final ExecutorService executor2 = Executors.newFixedThreadPool(1);
        final var generator1 = new ByteGenerator(eventsPerSecond, timeInSeconds, executor1, 1);
        final var generator2 = new ByteGenerator(eventsPerSecond, timeInSeconds, executor2, 2);
        log.printf(Level.INFO, "Expecting %,d join tupel",
                generator1.expectedJoinSize(generator2, eventsPerSecond, timeInSeconds, 1));

        try {
            AbstractSerializer<byte[]> serializerInstance = new ByteSerializer();

            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";
            String socket2File = DirectoryHelper.getOutputPath() + "socket2.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File, this.timeInSeconds,
                    (int) eventsPerSecond);
            var s2 = new BlockingSocket<>(basicPort2, serializerInstance, socket2File, this.timeInSeconds,
                    (int) eventsPerSecond);
            s1.setByteFlag(true);
            s2.setByteFlag(true);
            s1.setByteLength(13);
            s2.setByteLength(13);
            s1.waitForConnection();
            s2.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            var done2 = generator2.generate(s2);
            CompletableFuture.allOf(done, done2).get();
            long endTime = System.nanoTime();
            s1.writeFile();
            s2.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void newNexmarkTwoSources() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor1 = Executors.newFixedThreadPool(1);
        final ExecutorService executor2 = Executors.newFixedThreadPool(1);
        final NexmarkLightDataGenerator dataGenerator = new NexmarkLightDataGenerator(1337);
        final var bidGenerator = new NexmarkByteBidGenerator(eventsPerSecond, timeInSeconds, executor1, dataGenerator);
        final var auctionGenerator = new NexmarkByteAuctionGenerator(eventsPerSecond, timeInSeconds, executor2,
                dataGenerator);
        try {
            AbstractSerializer<byte[]> serializerInstance = new ByteSerializer();

            String socket1File = DirectoryHelper.getOutputPath() + "socketBid.csv";
            String socket2File = DirectoryHelper.getOutputPath() + "socketAuction.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var bidSocket = new BlockingSocket<>(bidNetworkSocketPort, serializerInstance, socket1File,
                    this.timeInSeconds, (int) eventsPerSecond);
            var auctionSocket = new BlockingSocket<>(auctionNetworkSocketPort, serializerInstance, socket2File,
                    this.timeInSeconds, (int) eventsPerSecond);
            bidSocket.setByteFlag(true);
            auctionSocket.setByteFlag(true);
            bidSocket.setByteLength(33);
            auctionSocket.setByteLength(29);
            bidSocket.waitForConnection();
            auctionSocket.waitForConnection();
            long startTime = System.nanoTime();
            var bidDone = bidGenerator.generate(bidSocket);
            var auctionDone = auctionGenerator.generate(auctionSocket);
            CompletableFuture.allOf(auctionDone, bidDone).get();
            long endTime = System.nanoTime();
            bidSocket.writeFile();
            auctionSocket.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void basicBenchmarkTwoSources() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final var generator1 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor, 1);
        final var generator2 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor, 2);
        log.printf(Level.INFO, "Expecting %,d join tupel",
                generator1.expectedJoinSize(generator2, eventsPerSecond, timeInSeconds, 5));

        try {
            AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";
            String socket2File = DirectoryHelper.getOutputPath() + "socket2.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File, this.timeInSeconds,
                    (int) eventsPerSecond);
            var s2 = new BlockingSocket<>(basicPort2, serializerInstance, socket2File, this.timeInSeconds,
                    (int) eventsPerSecond);
            s1.waitForConnection();
            s2.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            var done2 = generator2.generate(s2);
            CompletableFuture.allOf(done, done2).get();
            long endTime = System.nanoTime();
            s1.writeFile();
            s2.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void nexmarkLightOneSource() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final NexmarkLightDataGenerator dataGenerator = new NexmarkLightDataGenerator(1337);

        final var auctionGenerator = new NexmarkLightAuctionGenerator(eventsPerSecond, timeInSeconds, executor,
                dataGenerator);
        try {
            var auctionSerializerInstance = new NexmarkLightAuctionSerializer();

            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";
            log.info("{}", basicPort2);
            var auctionSocket = new BlockingSocket<>(basicPort2, auctionSerializerInstance, socket1File,
                    this.timeInSeconds, (int) eventsPerSecond);
            auctionSocket.waitForConnection();
            long startTime = System.nanoTime();
            var done = auctionGenerator.generate(auctionSocket);
            done.get();
            long endTime = System.nanoTime();
            auctionSocket.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void nexmarkLightTwoSources() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d", eventsPerSecond, timeInSeconds,
                eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final NexmarkLightDataGenerator dataGenerator = new NexmarkLightDataGenerator(1337);

        final var bidGenerator1 = new NexmarkLightBidGenerator(eventsPerSecond, timeInSeconds, executor, dataGenerator);
        final var auctionGenerator2 = new NexmarkLightAuctionGenerator(eventsPerSecond, timeInSeconds, executor,
                dataGenerator);

        try {
            var bidSerializerInstance = new NexmarkLightBidSerializer();
            var auctionSerializerInstance = new NexmarkLightAuctionSerializer();

            String socket1File = DirectoryHelper.getOutputPath() + "socket1.csv";
            String socket2File = DirectoryHelper.getOutputPath() + "socket2.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var bidSocket = new BlockingSocket<>(basicPort1, bidSerializerInstance, socket1File, this.timeInSeconds,
                    (int) eventsPerSecond);
            var auctionSocket = new BlockingSocket<>(basicPort2, auctionSerializerInstance, socket2File,
                    this.timeInSeconds, (int) eventsPerSecond);
            bidSocket.waitForConnection();
            auctionSocket.waitForConnection();
            long startTime = System.nanoTime();
            var done = bidGenerator1.generate(bidSocket);
            var done2 = auctionGenerator2.generate(auctionSocket);
            CompletableFuture.allOf(done, done2).get();
            long endTime = System.nanoTime();
            bidSocket.writeFile();
            auctionSocket.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
