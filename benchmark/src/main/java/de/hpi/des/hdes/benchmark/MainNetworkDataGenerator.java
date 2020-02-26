package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.NetworkGenerator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class MainNetworkDataGenerator implements Runnable {
    @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "1000")
    private int eventsPerSecond;
    @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "60")
    private int timeInSeconds;
    @Option(names = {"--personFraction", "-pf"}, defaultValue = "0.05")
    private double personFraction;
    @Option(names = {"--auctionFraction", "-af"}, defaultValue = "0.35")
    private double auctionFraction;
    @Option(names = {"--bidFraction", "-bf"}, defaultValue = "0.60")
    private double bidFraction;
    @Option(names = {"--engineHost", "-eh"}, defaultValue = "192.168.0.19")
    private String engineHost;
    @Option(names = {"--auctionSourcePort", "-asp"}, defaultValue = "5551")
    private int auctionNetworkSocketPort;
    @Option(names = {"--bidSourcePort", "-bsp"}, defaultValue = "5552")
    private int bidNetworkSocketPort;
    @Option(names = {"--personSourcePort", "-psp"}, defaultValue = "5553")
    private int personNetworkSocketPort;

    public static void main(final String[] args) {
        new CommandLine(new MainNetworkDataGenerator()).execute(args);
    }

    @Override
    public void run() {

        final var generator = new NetworkGenerator(eventsPerSecond,
                timeInSeconds, this.personFraction, this.auctionFraction, this.bidFraction, engineHost,
                auctionNetworkSocketPort, bidNetworkSocketPort, personNetworkSocketPort);

        log.info("Running with {} EPS for {}s.", eventsPerSecond, timeInSeconds);
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
