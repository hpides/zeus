package de.hpi.des.hdes.engine.generators;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.temp.ajoin.AJoin;
import de.hpi.des.hdes.engine.temp.ajoin.Dispatcher;

public class AJoinTest {
    // @Test
    // public void testAJoinWriteOutput() {
    // ByteBuffer leftInput = ByteBuffer.allocate(64);
    // ByteBuffer rightInput = ByteBuffer.allocate(64);
    // for (int i = 0; i < 4; i++) {
    // leftInput.putLong(i).putInt(i * 10).putInt(i * 10 + 1);
    // rightInput.putLong(i).putInt(i * 10).putInt(i * 10 + 1);
    // }
    // ByteBuffer output = ByteBuffer.allocate(256);
    // AJoin aJoin = new AJoin(leftInput, rightInput, output);
    // aJoin.writeOutput(24, 40);
    // }

    @Test
    public void testAJoinWindowing() {
        ByteBuffer leftInput = ByteBuffer.allocate(1400);
        ByteBuffer rightInput = leftInput.duplicate();
        ByteBuffer output = ByteBuffer.allocate(2500);
        for (int i = 1; i < 8; i++) {
            leftInput.putLong((long) i).putInt(i * 10).putInt(i * 10 + 1).putChar((char) 256);
        }
        leftInput.putLong((long) 8).putInt(8 * 10).putInt(8 * 10 + 1).putChar((char) 1);
        AJoin aJoin = new AJoin(leftInput, rightInput, new Dispatcher() {

            @Override
            public boolean write(Runnable p, byte[] out) {
                output.put(out);
                return true;
            }

            @Override
            public void resetLimit(ByteBuffer leftInput) {
                System.out.println("Reset limit");
            }

            @Override
            public void free(ByteBuffer buffer, int offset) {
                System.out.println("Freed " + offset);
            }
        }, 5, 5);
        leftInput.position(0);
        rightInput.position(0);
        for (int i = 0; i < 8; i++) {
            aJoin.readEventLeft();
            aJoin.readEventRight();
        }
        output.position(0);
        System.out.println("Done");
    }
}
