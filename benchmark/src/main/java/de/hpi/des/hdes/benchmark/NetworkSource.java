package de.hpi.des.hdes.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.SerializerFactory.FieldSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.IOException;
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

    private final Kryo kryo = new Kryo();
    private FieldSerializerFactory serializerFactory = new FieldSerializerFactory();

    public NetworkSource(int initialCapacity, int port, Class<E> clazz) {
        this.serializerFactory.getConfig().setFieldsCanBeNull(false);
        serializerFactory.getConfig().setVariableLengthEncoding(false);
        serializerFactory.getConfig().setFixedFieldTypes(true);
        kryo.setDefaultSerializer(serializerFactory);
        this.kryo.register(Person.class);
        this.kryo.register(Auction.class);
        this.kryo.register(Bid.class);

        this.queue = new SizedChunkedBuffer<>(initialCapacity);
        this.id = UUID.randomUUID();
        this.port = port;
        this.clazz = clazz;
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

            DataInputStream inputStream = new DataInputStream(objectSocket.getInputStream());

            int byteBufferSize;
            while ((byteBufferSize = inputStream.readInt()) >= 1 && !exit) {
                Object object;
                try {
                    byte[] byteBuffer = new byte[byteBufferSize];
                    inputStream.read(byteBuffer);
                    Input input = new Input(byteBuffer);
                    object = kryo.readClassAndObject(input);
                }
                catch (KryoException e) {
                    log.info("Encountered broken object, dropping it");
                    continue;
                }
                E castObject = (E) object;
                try {
                    queue.add(AData.of(castObject));
                } catch (IllegalStateException e) {
                    queue.drop();
                    log.warn("Dropped input buffer of {}", object.getClass());
                }
            }
        } catch (IOException e) {
            log.info("Network Source was shutdown or crashed unexpectedly");
        }
    }

    public void stop(){
        this.exit = true;
        try {
            this.objectSocket.close();
        } catch (IOException e) {
            log.info("Unexpected Exception occured: "+e.getMessage());
        }
    }
}
