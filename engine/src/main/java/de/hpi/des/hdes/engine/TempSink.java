package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.slot.Event;

public class TempSink {
    public void process(Event e) {
        System.out.println(e);
    }
}