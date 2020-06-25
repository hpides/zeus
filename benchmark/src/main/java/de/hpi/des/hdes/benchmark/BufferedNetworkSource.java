package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.execution.slot.Event;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.operation.AbstractSource;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.jooq.lambda.tuple.Tuple2;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class BufferedNetworkSource<T> implements BufferedSource {

    private final String host;
    private final SizedChunkedBuffer<Event> queue;
    private final AbstractSerializer<T> deserializer;
    private final String serializer;
    private UUID id;
    private int port;
    private boolean exit = false;
    private Socket objectSocket;
    private NetworkBuffer buffer;
    private Function<T, Event> eventConstructor;

    public BufferedNetworkSource(int maxBufferSize, int port, AbstractSerializer<T> serializerInstance, String host,
            String serializer, TimestampExtractor<T> timestampExtractor, Function<T, Event> eventConstructor) {
        this.queue = new SizedChunkedBuffer<>(maxBufferSize);
        this.id = UUID.randomUUID();
        this.port = port;
        this.deserializer = serializerInstance;
        this.host = host;
        this.serializer = serializer;
        this.eventConstructor = eventConstructor;
        // this.buffer = new NetworkBuffer(this.queue);

    }

    public void run() {
        log.info("Client server on {}", port);
        var isConnected = false;
        try {
            while (!isConnected) {
                try {
                    objectSocket = new Socket(this.host, this.port);
                    isConnected = true;
                } catch (ConnectException ignore) {
                }
            }
            log.info("Client connected on {} at {}", this.host, this.port);

            if (this.serializer.equals("json") || this.serializer.equals("custom") || this.serializer.equals("gson")) {
                BufferedReader in = new BufferedReader(new InputStreamReader(objectSocket.getInputStream()));

                String objectInput;
                while ((objectInput = in.readLine()) != null && !exit) {
                    T object = this.deserializer.deserialize(objectInput);

                    if (object == null) {
                        log.warn("null");
                        continue;
                    }
                    try {
                        while (queue.isFull()) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                return;
                            }
                            log.trace("Engine Socket has finished waiting");

                        }
                        queue.add(this.eventConstructor.apply(object));

                    } catch (IllegalStateException e) {
                        queue.drop();
                        log.error("Dropped input buffer of {}", object.getClass());
                    }
                }
            } else {
                log.error("Could not find a valid serializer");
                System.exit(-1);
            }
        } catch (IOException e) {
            log.info("Network Source was shutdown or crashed unexpectedly", e);
        }
    }

    public void stop() {
        this.exit = true;
        try {
            this.objectSocket.close();
        } catch (IOException e) {
            log.info("Unexpected Exception occured: " + e.getMessage());
        }
        log.info("Currently {} elements in entry queue", queue.size());
        Thread.currentThread().interrupt();
    }

    public void shutdown() {
        // TODO Auto-generated method stub

    }

    @Override
    public Buffer getInputBuffer() {
        return new Buffer() {
            private SizedChunkedBuffer<Event> bufferLeft = getQueue();

            @Override
            public List<Event> poll() {
                List<Event> result = new ArrayList<Event>(10);
                for (int i = 0; i < 10; i++) {
                    result.add(bufferLeft.poll());
                }
                return result;
            }

            @Override
            public void write(Event event) {
                // TODO Auto-generated method stub

            }
        };
    }
}