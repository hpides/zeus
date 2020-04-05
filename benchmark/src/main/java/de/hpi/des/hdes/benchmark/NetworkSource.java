package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractSource;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class NetworkSource<E> extends AbstractSource<E> implements Source<E>, Runnable {

  private final String host;
  private final SizedChunkedBuffer<E> queue;
  private final AbstractSerializer<E> deserializer;
  private final String serializer;
  private final Class<E> clazz;
  private UUID id;
  private int port;
  private boolean exit = false;
  private Socket objectSocket;


  public NetworkSource(int maxBufferSize, int port, AbstractSerializer<E> deserializer,
      String host, String serializer, Class<E> clazz,
      TimestampExtractor<E> timestampExtractor) {
    super(timestampExtractor, WatermarkGenerator.milliseconds(1, 100_000));
    this.queue = new SizedChunkedBuffer<>(maxBufferSize);
    this.id = UUID.randomUUID();
    this.port = port;
    this.deserializer = deserializer;
    this.host = host;
    this.serializer = serializer;
    this.clazz = clazz;

  }

  @Override
  public String getIdentifier() {
    return this.id.toString();
  }

  @Override
  public E readEvent() {
    return queue.poll();
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

      if (this.serializer.equals("json") ||
          this.serializer.equals("custom") ||
          this.serializer.equals("gson")) {
        BufferedReader in = new BufferedReader(
            new InputStreamReader(objectSocket.getInputStream()));

        String objectInput;
        while ((objectInput = in.readLine()) != null && !exit) {
          E object = this.deserializer.deserialize(objectInput);

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
            queue.add(object);

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
}
