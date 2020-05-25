package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.io.Buffer;

public interface BufferedSource extends RunnablePipeline {
    public Buffer getInputBuffer();
}