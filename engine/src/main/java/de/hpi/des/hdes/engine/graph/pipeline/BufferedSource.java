package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.io.Buffer;

public interface BufferedSource {
    public Buffer getInputBuffer();
}