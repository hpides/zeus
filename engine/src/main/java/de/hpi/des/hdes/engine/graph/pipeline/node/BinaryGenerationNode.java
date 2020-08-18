package de.hpi.des.hdes.engine.graph.pipeline.node;

import java.util.stream.Stream;

import de.hpi.des.hdes.engine.generators.BinaryGeneratable;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

@Getter
public abstract class BinaryGenerationNode extends GenerationNode {

  final private GenerationNode rightParent;
  final private PrimitiveType[] rightInputTypes;

  public BinaryGenerationNode(final PrimitiveType[] leftTypes, final PrimitiveType[] rightTypes,
      final BinaryGeneratable operator, final GenerationNode rightParent) {
    super(leftTypes, Stream.concat(Stream.of(leftTypes), Stream.of(rightTypes)).toArray(PrimitiveType[]::new),
        operator);
    this.rightParent = rightParent;
    this.rightInputTypes = rightTypes;
  }
}