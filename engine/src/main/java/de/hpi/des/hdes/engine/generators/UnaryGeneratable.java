package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;

public interface UnaryGeneratable extends Generatable {
  String generate(Pipeline pipeline, boolean isRight);
}
