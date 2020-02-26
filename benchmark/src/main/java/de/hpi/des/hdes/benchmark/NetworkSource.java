package de.hpi.des.hdes.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

@Getter
@Slf4j
public class NetworkSource<E> extends AbstractTopologyElement<E> implements Source<E>, Runnable {
    private SizedChunkedBuffer<AData<E>> queue;
    private UUID id;
    private int port;
    private Class<E> clazz;
    private boolean exit = false;
    private Socket objectSocket;
    private final ObjectMapper objectMapper;

    public NetworkSource(int initialCapacity, int port, Class<E> clazz) {
        this.queue = new SizedChunkedBuffer<>(initialCapacity);
        this.id = UUID.randomUUID();
        this.port = port;
        this.clazz = clazz;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String getIdentifier() {
        return this.id.toString();
    }

    @Override
    public void read() {
        AData<E> event = queue.poll();
        if (event != null) {
            collector.collect(event);
        }
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            this.objectSocket = serverSocket.accept();
            BufferedReader in = new BufferedReader(new InputStreamReader(objectSocket.getInputStream()));

            String objectInput;
            while ((objectInput = in.readLine()) != null && !exit) {
                E object = this.objectMapper.readValue(objectInput, this.clazz);

                if (object == null) {
                    log.warn("null");
                    continue;
                }
                try {
                    queue.add(AData.of(object));
                } catch (IllegalStateException e) {
                    queue.drop();
                    log.warn("Dropped input buffer of {}", object.getClass());
                }
            }
        } catch (IOException e) {
            log.info("Network Source was shutdown or crashed unexpectedly");
        }
    }

    public void stop() {
        this.exit = true;
        try {
            this.objectSocket.close();
        } catch (IOException e) {
            log.info("Unexpected Exception occured: " + e.getMessage());
        }
    }
}
