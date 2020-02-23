import de.hpi.des.hdes.benchmark.BlockingSource;
import de.hpi.des.hdes.benchmark.generator.InMemoryNexGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.util.concurrent.ExecutionException;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
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

  public void run() {
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
      //generator.generate();
      long startTime = System.nanoTime();
      env.execute("Fraud Detection");
      long endTime = System.nanoTime();
      log.warn("Finished after {} seconds.", (endTime - startTime) / 1e9);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}