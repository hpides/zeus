package de.hpi.des.hdes.benchmark.generator;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.benchmark.BlockingSource;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.util.concurrent.ExecutionException;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;

public class NexGeneratorTest {

  @Test
  void shouldGenerateSameDataEachTime() {
    final double personFraction = 0.05;
    final double auctionFraction = 0.35;
    final double bidFraction = 0.6;
    final int eventsPerSecond = 10_000;
    final int maxDelayInSeconds = 100_000; // avoid drops

    final var personSource = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final var auctionSource = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final var bidSource = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    var nx1 = new InMemoryNexGenerator(auctionSource, bidSource, personSource, eventsPerSecond,
        10, personFraction, auctionFraction, bidFraction);
    final var personSource2 = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final var auctionSource2 = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final var bidSource2 = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    var nx2 = new InMemoryNexGenerator(auctionSource2, bidSource2, personSource2, eventsPerSecond,
        10, personFraction, auctionFraction, bidFraction);
    try {
      nx1.prepare().get();
      var done1 = nx1.generate();
      done1.get();
      nx2.prepare().get();
      var done2 = nx2.generate();
      done2.get();
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    Seq.seq(personSource.getQueue().unsafePollAll()).zip(personSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));
    Seq.seq(auctionSource.getQueue().unsafePollAll()).zip(auctionSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));
    Seq.seq(bidSource.getQueue().unsafePollAll()).zip(bidSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));

  }

  @Test
  void inMemoryAndDiskShouldBehaveTheSame() {
    final double personFraction = 0.05;
    final double auctionFraction = 0.35;
    final double bidFraction = 0.6;
    final int eventsPerSecond = 10_000;
    final int maxDelayInSeconds = 100_000; // avoid drops

    final var personSource = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final var auctionSource = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final var bidSource = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    var nx1 = new InMemoryNexGenerator(auctionSource, bidSource, personSource, eventsPerSecond,
        10, personFraction, auctionFraction, bidFraction);
    final var personSource2 = new BlockingSource<Person>(
        (int) (personFraction * eventsPerSecond * maxDelayInSeconds));
    final var auctionSource2 = new BlockingSource<Auction>(
        (int) (auctionFraction * eventsPerSecond * maxDelayInSeconds));
    final var bidSource2 = new BlockingSource<Bid>(
        (int) (bidFraction * eventsPerSecond * maxDelayInSeconds));
    var nx2 = new DiskNexGenerator(auctionSource2, bidSource2, personSource2, eventsPerSecond,
        10, personFraction, auctionFraction, bidFraction);
    try {
      nx1.prepare().get();
      var done1 = nx1.generate();
      done1.get();
      nx2.prepare().get();
      var done2 = nx2.generate();
      done2.get();
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    Seq.seq(personSource.getQueue().unsafePollAll()).zip(personSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));
    Seq.seq(auctionSource.getQueue().unsafePollAll()).zip(auctionSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));
    Seq.seq(bidSource.getQueue().unsafePollAll()).zip(bidSource2.getQueue().unsafePollAll())
        .forEach(t -> assertThat(t.v1.getValue()).isEqualTo(t.v2.getValue()));

  }

}
