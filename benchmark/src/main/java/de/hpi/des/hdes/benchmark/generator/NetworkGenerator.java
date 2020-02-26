package de.hpi.des.hdes.benchmark.generator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory.FieldSerializerFactory;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.des.hdes.benchmark.nexmark.ObjectAuctionStreamGenerator;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class NetworkGenerator {
    private final int eventsPerSecond;
    private final int timeInSeconds;
    private final double fractionPersons;
    private final double fractionAction;
    private final double fractionBid;

    private final Kryo kryo = new Kryo();
    private FieldSerializerFactory serializerFactory = new FieldSerializerFactory();
    private ObjectAuctionStreamGenerator objectAuctionStreamGenerator;
    private final String engineIp;
    private final int auctionNetworkSocketPort;
    private final int bidNetworkSocketPort;
    private final int personNetworkSocketPort;
    private ObjectMapper objectMapper;

    private boolean engineRunning = true;

    private void init() {
        this.serializerFactory.getConfig().setFieldsCanBeNull(false);
        this.serializerFactory.getConfig().setVariableLengthEncoding(false);
        this.serializerFactory.getConfig().setFixedFieldTypes(true);
        this.kryo.setDefaultSerializer(serializerFactory);
        this.kryo.register(Person.class);
        this.kryo.register(Auction.class);
        this.kryo.register(Bid.class);
        this.objectAuctionStreamGenerator = new ObjectAuctionStreamGenerator();
        this.objectMapper = new ObjectMapper();
    }

    public CompletableFuture<Boolean> generate() {
        return CompletableFuture.supplyAsync(this::sendEventsTimeAware);
    }

    private boolean sendEventsTimeAware() {
        this.init();
        try {
            Socket personSocket = new Socket(this.engineIp, this.personNetworkSocketPort);
            Socket auctionSocket = new Socket(this.engineIp, this.auctionNetworkSocketPort);
            Socket bidSocket = new Socket(this.engineIp, this.bidNetworkSocketPort);

            OutputStreamWriter auctionOut = new OutputStreamWriter(auctionSocket.getOutputStream(), StandardCharsets.UTF_8);
            OutputStreamWriter bidOut = new OutputStreamWriter(bidSocket.getOutputStream(), StandardCharsets.UTF_8);
            OutputStreamWriter personOut = new OutputStreamWriter(personSocket.getOutputStream(), StandardCharsets.UTF_8);

            long sentEvents = 0;

            final int totalEvents = eventsPerSecond * timeInSeconds;
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

                nrPersons += eventsToBeSent * this.fractionPersons;
                nrAuctions += eventsToBeSent * this.fractionAction;
                nrBids += eventsToBeSent * this.fractionBid;

                long sentPersons = sendJSONViaSocket(nrPersons, personOut, objectAuctionStreamGenerator::generatePerson);
                long sentAuctions = sendJSONViaSocket(nrAuctions, auctionOut, objectAuctionStreamGenerator::generateAuction);
                long sentBids = sendJSONViaSocket(nrBids, bidOut, objectAuctionStreamGenerator::generateBid);

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

    private <T> long sendJSONViaSocket(double numEvents, OutputStreamWriter out, Supplier<T> generator) {
        if (numEvents < 1) {
            return 0;
        }
        long events = (long) Math.floor(numEvents);
        try {
            for (int i = 0; i < events; i++) {
                T event = generator.get();

                String jsonString = objectMapper.writeValueAsString(event).concat("\n");
                out.write(jsonString);
                out.flush();
            }
        } catch (IOException e) {
            if(e instanceof SocketException) {
                log.error("SocketException: Remote Host closed connection, Engine may be finished. Stopping");
                this.engineRunning = false;
            }
            else {
                e.printStackTrace();
            }
        }
        return events;
    }

}