package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.AbstractSerializer;
import de.hpi.des.hdes.benchmark.nexmark.ObjectAuctionStreamGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class NetworkGenerator {

  private final long eventsPerSecond;
  private final long timeInSeconds;
  private final double fractionPersons;
  private final double fractionAction;
  private final double fractionBid;
  private final int auctionNetworkSocketPort;
  private final int bidNetworkSocketPort;
  private final int personNetworkSocketPort;
  private final AbstractSerializer<Auction> serializerAuction;
  private final AbstractSerializer<Bid> serializerBid;
  private final AbstractSerializer<Person> serializerPerson;
  private ObjectAuctionStreamGenerator objectAuctionStreamGenerator;
  private boolean engineRunning = true;
  private final String serializer;

  private void init() {
    this.objectAuctionStreamGenerator = new ObjectAuctionStreamGenerator();
  }

  public CompletableFuture<Boolean> generate() {
    return CompletableFuture.supplyAsync(this::sendEventsTimeAware);
  }

  private boolean sendEventsTimeAware() {
    this.init();
    try {
      var pserver = new ServerSocket(this.personNetworkSocketPort);
      var aserver = new ServerSocket(this.auctionNetworkSocketPort);
      var bserver = new ServerSocket(this.bidNetworkSocketPort);
      var personSocket = pserver.accept();
      var auctionSocket = aserver.accept();
      var bidSocket = bserver.accept();

      log.info("Connected to Engine. Starting Data Generation");

      OutputStreamWriter auctionOut = new OutputStreamWriter(auctionSocket.getOutputStream(),
          StandardCharsets.UTF_8);
      OutputStreamWriter bidOut = new OutputStreamWriter(bidSocket.getOutputStream(),
          StandardCharsets.UTF_8);
      OutputStreamWriter personOut = new OutputStreamWriter(personSocket.getOutputStream(),
          StandardCharsets.UTF_8);

      long sentEvents = 0;

      final long totalEvents = eventsPerSecond * timeInSeconds;
      final long startTime = System.nanoTime();
      double nrPersons = 0.0d;
      double nrAuctions = 0.0d;
      double nrBids = 0.0d;
      while (sentEvents < totalEvents && this.engineRunning) {
        final long timeNow = System.nanoTime();
        final long nanoDifference = timeNow - startTime;
        final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
        final long missingEvents = currentEventTarget - sentEvents;
        final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);
        log.trace("Events to be sent {}", eventsToBeSent);
        nrPersons += eventsToBeSent * this.fractionPersons;
        nrAuctions += eventsToBeSent * this.fractionAction;
        nrBids += eventsToBeSent * this.fractionBid;

        long sentPersons = 0;
        long sentAuctions = 0;
        long sentBids = 0;
        if(this.serializer.equals("gson")) {
          sentPersons = sendJSONViaSocket(nrPersons, personOut, objectAuctionStreamGenerator::generatePerson, serializerPerson);
          sentAuctions = sendJSONViaSocket(nrAuctions, auctionOut, objectAuctionStreamGenerator::generateAuction, serializerAuction);
          sentBids = sendJSONViaSocket(nrBids, bidOut, objectAuctionStreamGenerator::generateBid, serializerBid);
        }
        else if(this.serializer.equals("protobuf")) {
          sentPersons = sendProtobufViaSocket(nrPersons, personSocket.getOutputStream(), objectAuctionStreamGenerator::generateProtobufPerson);
          sentAuctions = sendProtobufViaSocket(nrAuctions, auctionSocket.getOutputStream(), objectAuctionStreamGenerator::generateProtobufAuction);
          sentBids = sendProtobufViaSocket(nrBids, bidSocket.getOutputStream(), objectAuctionStreamGenerator::generateProtobufBid);
        }
        else {
          log.error("Could not find any seralizer with name: " + this.serializer);
          System.exit(-1);
        }

        nrPersons -= sentPersons;
        nrAuctions -= sentAuctions;
        nrBids -= sentBids;

        sentEvents += sentPersons + sentAuctions + sentBids;
      }
      log.info("Sent {} events", sentEvents);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

  private <T> long sendJSONViaSocket(double numEvents, OutputStreamWriter out,
      Supplier<T> generator, AbstractSerializer<T> serializer) {
    if (numEvents < 1) {
      return 0;
    }
    long events = (long) Math.floor(numEvents);
    int i = 0;
    try {
      for (; i < events; i++) {
        T event = generator.get();

        String jsonString = serializer.serialize(event).concat("\n");
        out.write(jsonString);
      }
      out.flush();
    } catch (IOException e) {
      if (e instanceof SocketException) {
        log.error(
            "SocketException: Remote Host closed connection, Engine may be finished. Stopping");
        this.engineRunning = false;
      } else {
        e.printStackTrace();
      }
    }
    return i;
  }

  private  long sendProtobufViaSocket(double numEvents,
                                      OutputStream out,
                                      Supplier<com.google.protobuf.GeneratedMessageV3> generator) {
    if (numEvents < 1) {
      return 0;
    }
    long events = (long) Math.floor(numEvents);
    int i = 0;
    try {
      for (; i < events; i++) {
        var event = generator.get();
        event.writeDelimitedTo(out);
      }
    } catch (IOException e) {
      if (e instanceof SocketException) {
        log.error(
                "SocketException: Remote Host closed connection, Engine may be finished. Stopping");
        this.engineRunning = false;
      } else {
        e.printStackTrace();
      }
    }
    return i;
  }

}
