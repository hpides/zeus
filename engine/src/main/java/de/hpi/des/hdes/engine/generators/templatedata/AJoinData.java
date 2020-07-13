package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

@Getter
public class AJoinData {
  private final String pipelineId;
  private final int leftTupleLength;
  private final int rightTupleLength;
  private final String keyType;
  private final String nativeKeyType;
  private final int leftKeyOffset;
  private final int rightKeyOffset;

  public AJoinData(final String pipelineId, final PrimitiveType[] leftTypes, final PrimitiveType[] rightTypes,
      final int leftKeyIndex, final int rightKeyIndex) {
    this.pipelineId = pipelineId;
    this.keyType = rightTypes[rightKeyIndex].getUppercaseName();
    this.nativeKeyType = rightTypes[rightKeyIndex].getLowercaseName();
    int leftOffset = 0;
    int leftSize = 0;
    for (int i = 0; i < leftTypes.length; i++) {
      int length = leftTypes[i].getLength();
      leftSize += length;
      if (i < leftKeyIndex)
        leftOffset += length;
    }
    this.leftKeyOffset = leftOffset;
    this.leftTupleLength = leftSize;
    int rightOffset = 0;
    int rightSize = 0;
    for (int i = 0; i < rightTypes.length; i++) {
      int length = rightTypes[i].getLength();
      rightSize += length;
      if (i < rightKeyIndex)
        rightOffset += length;
    }
    this.rightKeyOffset = rightOffset;
    this.rightTupleLength = rightSize;
  }
}