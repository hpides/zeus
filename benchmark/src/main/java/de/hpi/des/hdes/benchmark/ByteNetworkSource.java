package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.execution.Dispatcher;
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
public class ByteNetworkSource implements Runnable {

    private final String host;
    private final AbstractSerializer<byte[]> deserializer;
    private UUID id;
    private int port;
    private boolean exit = false;
    private Socket objectSocket;
    private NetworkBuffer buffer;
    private final Dispatcher dispatcher;
    private final String pipeline;

    public ByteNetworkSource(int maxBufferSize, int port, AbstractSerializer<byte[]> serializerInstance, String host,
            Dispatcher dispatcher, String pipeline) {
        this.id = UUID.randomUUID();
        this.port = port;
        this.deserializer = serializerInstance;
        this.host = host;
        this.dispatcher = dispatcher;
        this.pipeline = pipeline;
        // this.buffer = new NetworkBuffer(this.queue);

    }

    @Override
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
            BufferedReader in = new BufferedReader(new InputStreamReader(objectSocket.getInputStream()));

            String objectInput;
            while ((objectInput = in.readLine()) != null && !exit) {
                byte[] object = this.deserializer.deserialize(objectInput);

                if (object == null) {
                    log.warn("null");
                    continue;
                }
                try {
                    while (!this.dispatcher.write(pipeline, object)) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            return;
                        }
                        log.trace("Engine Socket has finished waiting");

                    }

                } catch (IllegalStateException e) {
                    log.error("Dropped input buffer of {}", object.getClass());
                }
            }
        } catch (IOException e) {
            log.info("Network Source was shutdown or crashed unexpectedly", e);
        }

    }

}