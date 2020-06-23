package de.hpi.des.hdes.engine.generators;

import java.io.FileOutputStream;
import java.util.Arrays;

import lombok.Getter;

public class AJoinGenerator implements BinaryGeneratable {

  private final int keyPositionLeft;
  private final int keyPositionRight;
  private final PrimitiveType[] leftTypes;
  private final PrimitiveType[] rightTypes;

  @Getter
  private class AJoinData {
    private final String pipelineId;
    private final int leftTupleLength;
    private final int rightTupleLength;
    private final String keyType;
    private final String nativeKeyType;

    public AJoinData(String pipelineId, PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, PrimitiveType keyType) {
      this.pipelineId = pipelineId;
      this.leftTupleLength = Arrays.stream(leftTypes).mapToInt(x -> x.getLength()).sum();
      this.rightTupleLength = Arrays.stream(rightTypes).mapToInt(x -> x.getLength()).sum();
      this.keyType = keyType.getUppercaseName();
      this.nativeKeyType = keyType.getLowercaseName();
    }
  }

  public AJoinGenerator(PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, int keyPositionLeft,
      int keyPositionRight) {
    this.leftTypes = leftTypes;
    this.rightTypes = rightTypes;
    this.keyPositionLeft = keyPositionLeft;
    this.keyPositionRight = keyPositionRight;
  }

  @Override
  public String generate(String execution, String nextPipelineFunction, boolean isLeft) {
    // TODO
    return null;
  }

}