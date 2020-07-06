package de.hpi.des.hdes.engine.generators;

import lombok.Getter;

@Getter
public class JoinGenerator implements BinaryGeneratable {

  private final int keyPositionLeft;
  private final int keyPositionRight;
  private final PrimitiveType[] leftTypes;
  private final PrimitiveType[] rightTypes;

  public JoinGenerator(PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, int keyPositionLeft,
      int keyPositionRight) {
    this.leftTypes = leftTypes;
    this.rightTypes = rightTypes;
    this.keyPositionLeft = keyPositionLeft;
    this.keyPositionRight = keyPositionRight;
  }

  @Override
  public String generate(String execution, String nextPipelineFunction, boolean isLeft) {
    // TODO
    return "";
  }
}