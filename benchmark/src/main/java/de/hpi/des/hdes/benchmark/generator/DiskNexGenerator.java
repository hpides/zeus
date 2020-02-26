package de.hpi.des.hdes.benchmark.generator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory.FieldSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.unsafe.UnsafeInput;
import de.hpi.des.hdes.benchmark.BlockingSource;
import de.hpi.des.hdes.benchmark.nexmark.ObjectAuctionStreamGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


@Log4j2
@RequiredArgsConstructor()
public class DiskNexGenerator {

  private final BlockingSource<Auction> auctionSource;
  private final BlockingSource<Bid> bidSource;
  private final BlockingSource<Person> personSource;
  private final int eventsPerSecond;
  private final int timeInSeconds;
  private final double fractionPersons;
  private final double fractionAction;
  private final double fractionBid;
  private final Kryo kryo = new Kryo();
  private String bidFilePath;
  private String auctionFilePath;
  private String personFilePath;
  private FieldSerializerFactory serializerFactory = new FieldSerializerFactory();


  private void init() {
    this.serializerFactory.getConfig().setFieldsCanBeNull(false);
    this.serializerFactory.getConfig().setVariableLengthEncoding(false);
    this.serializerFactory.getConfig().setFixedFieldTypes(true);
    this.kryo.setDefaultSerializer(serializerFactory);
    this.kryo.register(Person.class);
    this.kryo.register(Auction.class);
    this.kryo.register(Bid.class);
  }

  public CompletableFuture<Boolean> prepare() {
    return CompletableFuture.supplyAsync(this::prepareEvents);
  }

  private boolean prepareEvents() {
    this.init();
    final int totalEvents = eventsPerSecond * timeInSeconds;
    var generator = new ObjectAuctionStreamGenerator();
    int chunkSize = Math.min(10_000, totalEvents);
    assert (totalEvents % 10_000) == 0;
    int steps = totalEvents / chunkSize;
    try {
      var personsFile = File.createTempFile("Persons", ".tmp");
      personsFile.deleteOnExit();
      var auctionsFile = File.createTempFile("Auctions", ".tmp");
      auctionsFile.deleteOnExit();
      var bidFile = File.createTempFile("Bids", ".tmp");
      bidFile.deleteOnExit();
      for (int i = 0; i < steps; i++) {
        this.personFilePath = this.generateForType(
            (long) (this.fractionPersons * chunkSize),
            personsFile,
            generator::generatePerson);
        this.auctionFilePath = this.generateForType(
            (long) (this.fractionAction * chunkSize),
            auctionsFile,
            generator::generateAuction);
        this.bidFilePath = this.generateForType(
            (long) (this.fractionBid * chunkSize),
            bidFile,
            generator::generateBid);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    log.info("finished preparing");

    return true;
  }

  public CompletableFuture<Boolean> generate() {
    return CompletableFuture.supplyAsync(this::sendEventsTimeAware);
  }

  private boolean sendEventsTimeAware() {
    try (
        var pis = new UnsafeInput(Files.newInputStream(Paths.get(this.personFilePath)),
            100_000_000);
        var ais = new UnsafeInput(Files.newInputStream(Paths.get(this.auctionFilePath)),
            100_000_000);
        var bis = new UnsafeInput(Files.newInputStream(Paths.get(this.bidFilePath)), 100_000_000)) {

      long sentEvents = 0;
      final int totalEvents = eventsPerSecond * timeInSeconds;
      final long startTime = System.nanoTime();
      double nrPersons = 0.0d;
      double nrAuctions = 0.0d;
      double nrBids = 0.0d;
      while (sentEvents < totalEvents) {
        final long timeNow = System.nanoTime();
        final long nanoDifference = timeNow - startTime;

        final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
        final long missingEvents = currentEventTarget - sentEvents;
        final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);
        nrPersons += eventsToBeSent * this.fractionPersons;
        nrAuctions += eventsToBeSent * this.fractionAction;
        nrBids += eventsToBeSent * this.fractionBid;

        long sentPersons = this.send(nrPersons, pis, Person.class, this.personSource);
        long sentAuctions = this.send(nrAuctions, ais, Auction.class, auctionSource);
        long sentBids = this.send(nrBids, bis, Bid.class, bidSource);
        nrPersons -= sentPersons;
        nrAuctions -= sentAuctions;
        nrBids -= sentBids;

        sentEvents += sentPersons + sentAuctions + sentBids;

      }
      log.info("Sent {} events", sentEvents);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.personSource.getQueue().flush();
    this.auctionSource.getQueue().flush();
    this.bidSource.getQueue().flush();
    log.info("Finished generating events.");

    return true;
  }

  private <T> long send(double nrEvents, Input input,
      Class<T> clazz,
      BlockingSource<T> source) {
    if (nrEvents >= 1) {
      long events = (long) Math.floor(nrEvents);
      for (int i = 0; i < events && !input.end(); i++) {
        try {
          T value = kryo.readObject(input, clazz);
          source.offer(value);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      return events;
    }
    return 0;
  }

  private String generateForType(
      long eventsToBeSent,
      File tempFile,
      Supplier<?> generator) {
    try {
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      Output oos = new Output(fos);
      for (int i = 0; i < eventsToBeSent; i++) {
        var event = generator.get();
        kryo.writeObject(oos, event);
      }
      oos.close();
      fos.close();
      return tempFile.getAbsolutePath();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "";
  }
}
