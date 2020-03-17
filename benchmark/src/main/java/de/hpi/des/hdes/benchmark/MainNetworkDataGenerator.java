package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.IntegerTupleGenerator;
import de.hpi.des.hdes.benchmark.generator.NetworkGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.jooq.lambda.tuple.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Log4j2
public class MainNetworkDataGenerator implements Runnable {
    @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "10000000")
    private long eventsPerSecond;
    @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "30")
    private long timeInSeconds;
    @Option(names = {"--personFraction", "-pf"}, defaultValue = "0.05")
    private double personFraction;
    @Option(names = {"--auctionFraction", "-af"}, defaultValue = "0.35")
    private double auctionFraction;
    @Option(names = {"--bidFraction", "-bf"}, defaultValue = "0.60")
    private double bidFraction;
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

    public static void main(final String[] args) {
        new CommandLine(new MainNetworkDataGenerator()).execute(args);
    }

    @Override
    public void run() {
        basicBenchmarkTwoSources();
    }

    private AbstractSerializer getSerializer(String benchmarkType) {
        if(benchmarkType.equals("basic")) {
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
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d",
                eventsPerSecond, timeInSeconds, eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final var generator1 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor, 1);

        try {
            AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");
            String socket1File = System.getProperty("user.home") + File.separator + "socket1.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File, this.timeInSeconds);
            s1.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            done.get();
            long endTime = System.nanoTime();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void basicBenchmarkTwoSources() {
        log.printf(Level.INFO, "Running with %,d EPS, %ds. In total %,d",
            eventsPerSecond, timeInSeconds, eventsPerSecond * timeInSeconds);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final var generator1 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor,
            1);
        final var generator2 = new IntegerTupleGenerator(eventsPerSecond, timeInSeconds, executor,
            2);
        log.printf(Level.INFO, "Expecting %,d join tupel",
            generator1.expectedJoinSize(generator2, eventsPerSecond, timeInSeconds, 5));

        try {
            AbstractSerializer<Tuple2<Integer, Long>> serializerInstance = getSerializer("basic");

            String socket1File = System.getProperty("user.home") + File.separator + "socket1.csv";
            String socket2File = System.getProperty("user.home") + File.separator + "socket2.csv";

            log.info("{} {}", basicPort1, basicPort2);
            var s1 = new BlockingSocket<>(basicPort1, serializerInstance, socket1File,
                this.timeInSeconds);
            var s2 = new BlockingSocket<>(basicPort2, serializerInstance, socket2File,
                this.timeInSeconds);
            s1.waitForConnection();
            s2.waitForConnection();
            long startTime = System.nanoTime();
            var done = generator1.generate(s1);
            var done2 = generator2.generate(s2);
            done.get();
            long endTime = System.nanoTime();
            s1.writeFile();
            s2.writeFile();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void nexmark() {
        AbstractSerializer<Auction> serializerInstanceAuction = null;
        AbstractSerializer<Bid> serializerInstanceBid = null;
        AbstractSerializer<Person> serializerInstancePerson = null;

        if(serializer.equals("gson")) {
            serializerInstanceAuction = GSONSerializer.forAuction();
            serializerInstanceBid = GSONSerializer.forBid();
            serializerInstancePerson = GSONSerializer.forPerson();
        }
        else if(serializer.equals("custom")) {
            log.error("Custom serializer is not supported for Nexmark. Exiting");
            System.exit(-1);
        }
        else if(serializer.equals("protobuf")) {
            log.info("Using Protobuf");
        }
        else {
            log.error("No serializer found with the given name. Exiting");
            System.exit(-1);
        }

        final var generator = new NetworkGenerator(eventsPerSecond,
                timeInSeconds, this.personFraction, this.auctionFraction, this.bidFraction,
                auctionNetworkSocketPort, bidNetworkSocketPort, personNetworkSocketPort,
                serializerInstanceAuction, serializerInstanceBid, serializerInstancePerson, serializer);

        log.printf(Level.INFO, "Running with %,d EPS for %ds s. Target: %,d", eventsPerSecond,
                timeInSeconds, (eventsPerSecond * timeInSeconds));
        long startTime = System.nanoTime();
        var done = generator.generate();

        try {
            done.get();
            long endTime = System.nanoTime();
            log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
