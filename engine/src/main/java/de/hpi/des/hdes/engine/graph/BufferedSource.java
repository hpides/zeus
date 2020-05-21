package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.io.Buffer;

public interface BufferedSource extends RunnablePipeline {
    public Buffer getInputBuffer();
}