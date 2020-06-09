package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.io.Buffer;

public interface BufferedSink extends RunnablePipeline {
    public Buffer getOutputBuffer();
}
