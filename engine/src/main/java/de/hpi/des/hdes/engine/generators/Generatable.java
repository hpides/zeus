package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;

public interface Generatable {

    String generate(Pipeline pipeline, String execution);

}
