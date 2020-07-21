package de.hpi.des.hdes.engine.graph.pipeline.node;

import java.util.stream.Stream;

import de.hpi.des.hdes.engine.generators.BinaryGeneratable;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

@Getter
public abstract class BinaryGenerationNode extends GenerationNode {
  final private PrimitiveType[] joinInputTypes;

  public BinaryGenerationNode(final PrimitiveType[] inputTypes, final PrimitiveType[] joinTypes,
      final BinaryGeneratable operator) {
    super(inputTypes, Stream.concat(Stream.of(inputTypes), Stream.of(joinTypes)).toArray(PrimitiveType[]::new),
        operator);
    this.joinInputTypes = joinTypes;
  }
}