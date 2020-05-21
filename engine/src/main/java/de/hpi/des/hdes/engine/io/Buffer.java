package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.execution.slot.Event;

public interface Buffer {
    public Event poll();
}