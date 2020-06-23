package de.hpi.des.hdes.engine.temp.ajoin;

import java.nio.ByteBuffer;

public interface Dispatcher {

    void free(ByteBuffer buffer, int offset);

    void resetLimit(ByteBuffer leftInput);

    boolean write(Runnable p, byte[] out);

}
