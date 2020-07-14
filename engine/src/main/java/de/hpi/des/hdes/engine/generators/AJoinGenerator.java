package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.Getter;

@Getter
public class AJoinGenerator implements Generatable {

  private final int keyPositionLeft;
  private final int keyPositionRight;
  private final PrimitiveType[] leftTypes;
  private final PrimitiveType[] rightTypes;

  public AJoinGenerator(PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, int keyPositionLeft,
      int keyPositionRight) {
    this.leftTypes = leftTypes;
    this.rightTypes = rightTypes;
    this.keyPositionLeft = keyPositionLeft;
    this.keyPositionRight = keyPositionRight;
  }

  @Override
  public String generate(Pipeline pipeline, String execution) {
    // TODO
    return "";
  }

  @Override
  public String getOperatorId() {
    String hashBase = "ajoin";
    for (PrimitiveType t : leftTypes) {
      hashBase.concat(t.name());
    }
    hashBase.concat(Integer.toString(keyPositionLeft));
    for (PrimitiveType t : rightTypes) {
      hashBase.concat(t.name());
    }
    hashBase.concat(Integer.toString(keyPositionRight));
    return hashBase;
  }

}