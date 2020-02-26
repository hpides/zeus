package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.JobManager;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainNetworkEngine implements Runnable {

    @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "500000")
    private int eventsPerSecond;
    @Option(names = {"--maxDelayInSeconds", "-mds"}, defaultValue = "20")
    private int maxDelayInSeconds;
    @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "20")
    private int timeInSeconds;
    @Option(names = {"--threads", "-t"}, defaultValue = "8")
    private int nThreads;
    @Option(names = {"--auctionSourcePort", "-asp"}, defaultValue = "5551")
    private int auctionNetworkSocketPort;
    @Option(names = {"--bidSourcePort", "-bsp"}, defaultValue = "5552")
    private int bidNetworkSocketPort;
    @Option(names = {"--personSourcePort", "-psp"}, defaultValue = "5553")
    private int personNetworkSocketPort;
    @Option(names = {"--exitAfterXSeconds", "-exit"}, defaultValue = "30")
    private int exitAfter;

    public static void main(final String[] args) {
        new CommandLine(new MainNetworkEngine()).execute(args);
    }

    @Override
    public void run() {
        // Be aware that the sources are not flushed and could therefore still have events in their buffer when execution is finished
        // The engine will exit after 60 Seconds
        final var auctionSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
            auctionNetworkSocketPort, Auction.class);
        final var bidSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
            bidNetworkSocketPort, Bid.class);
        final var personSource = new NetworkSource<>(eventsPerSecond * maxDelayInSeconds,
            personNetworkSocketPort, Person.class);

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
