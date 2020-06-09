package de.hpi.des.hdes.engine.io;

import java.util.List;

import de.hpi.des.hdes.engine.execution.slot.Event;

public interface Buffer {
    public List<Event> poll();

    public void write(Event event);
}
