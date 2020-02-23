package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.BlockingSource;
import de.hpi.des.hdes.benchmark.nexmark.ObjectAuctionStreamGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;


@Log4j2
@RequiredArgsConstructor
public class InMemoryNexGenerator {

  private final BlockingSource<Auction> auctionSource;
  private final BlockingSource<Bid> bidSource;
  private final BlockingSource<Person> personSource;
  private final int eventsPerSecond;
  private final int timeInSeconds;
  private final double fractionPersons;
  private final double fractionAction;
  private final double fractionBid;
  @Getter
  private final ArrayList<Person> personBuffer = new ArrayList<>(1_000_000);
  @Getter
  private final ArrayList<Auction> auctionBuffer = new ArrayList<>(1_000_000);
  @Getter
  private final ArrayList<Bid> bidBuffer = new ArrayList<>(1_000_000);


  public CompletableFuture<Boolean> prepare() {
    return CompletableFuture.supplyAsync(this::prepareEvents);
  }

  private boolean prepareEvents() {
    final int totalEvents = eventsPerSecond * timeInSeconds;
    var generator = new ObjectAuctionStreamGenerator();
    int chunkSize = Math.min(10_000, totalEvents);
    assert (totalEvents % 10_000) == 0;
    int steps = totalEvents / chunkSize;
    for (int i = 0; i < steps; i++) {
      this.generateForType(
          (int) (this.fractionPersons * chunkSize),
          this.personBuffer,
          generator::generatePerson);
      this.generateForType(
          (int) (this.fractionAction * chunkSize),
          this.auctionBuffer,
          generator::generateAuction);
      this.generateForType(
          (int) (this.fractionBid * chunkSize),
          this.bidBuffer,
          generator::generateBid);
    }

    log.info("finished preparing");

    return true;
  }

  public CompletableFuture<Boolean> generate() {
    return CompletableFuture.supplyAsync(this::sendEventsTimeAware);
  }

  private boolean sendEventsTimeAware() {

    long sentEvents = 0;
    final int totalEvents = eventsPerSecond * timeInSeconds;
    final long startTime = System.nanoTime();
    double nrPersons = 0.0d;
    double nrAuctions = 0.0d;
    double nrBids = 0.0d;
    int personIndex = 0;
    int auctionIndex = 0;
    int bidsIndex = 0;
    while (sentEvents < totalEvents) {
      final long timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - sentEvents;
      final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);
      nrPersons += eventsToBeSent * this.fractionPersons;
      nrAuctions += eventsToBeSent * this.fractionAction;
      nrBids += eventsToBeSent * this.fractionBid;
      long sentPersons = this.send((int) Math.floor(nrPersons), personBuffer, personIndex,
          this.personSource);
      personIndex += sentPersons;
      long sentAuctions = this.send((int) Math.floor(nrAuctions), auctionBuffer, auctionIndex,
          this.auctionSource);
      auctionIndex += sentAuctions;
      long sentBids = this.send((int) Math.floor(nrBids), bidBuffer, bidsIndex,
          this.bidSource);
      bidsIndex += sentBids;

      nrPersons -= sentPersons;
      nrAuctions -= sentAuctions;
      nrBids -= sentBids;

      sentEvents += sentPersons + sentAuctions + sentBids;
    }
    log.info("Sent {} events", sentEvents);
    this.personSource.getQueue().flush();
    this.auctionSource.getQueue().flush();
    this.bidSource.getQueue().flush();
    log.info("Finished generating events.");

    return true;

  }

  private <T> long send(int nrEvents, ArrayList<T> buffer, int index,
      BlockingSource<T> source) {
    if (nrEvents >= 1) {
      for (int i = 0; i < nrEvents; i++) {
        source.offer(buffer.get(index++));
      }
      return nrEvents;
    }
    return 0;
  }


  private <T> String generateForType(
      int eventsToBeSent,
      ArrayList<T> buffer,
      Supplier<T> generator) {

    var quickBuff = new ArrayList<T>(eventsToBeSent);
    for (int i = 0; i < eventsToBeSent; i++) {
      var event = generator.get();
      quickBuff.add(event);
    }
    buffer.addAll(quickBuff);

    return "";
  }

}
