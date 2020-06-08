package de.hpi.des.hdes.engine.generators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.jooq.lambda.tuple.Tuple4;

import de.hpi.des.hdes.engine.execution.slot.Event;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TempSink {

    private BufferedWriter bw;

    public TempSink(FileWriter fw) {
        this.bw = new BufferedWriter(fw);
    }

    public void process(Event e) {
        try {
            bw.write(e.getTimestamp() + "," + System.currentTimeMillis() + '\n');
        } catch (IOException ex) {
            // exception handling left as an exercise for the reader
        }
    }

    public void flush() {
        try {
            this.bw.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}