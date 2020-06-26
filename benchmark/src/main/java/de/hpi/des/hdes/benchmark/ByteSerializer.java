package de.hpi.des.hdes.benchmark;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.jooq.lambda.tuple.Tuple4;

public class ByteSerializer extends AbstractSerializer<byte[]> {

    @Override
    public String serialize(byte[] obj) {
        try {
            return new String(obj, "ISO-8859-1") + "\n";
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public byte[] deserialize(String obj) {
        // TODO Auto-generated method stub
        try {
            return obj.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}