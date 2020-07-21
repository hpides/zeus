package de.hpi.des.hdes.engine.generators;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public abstract class BinaryGeneratable implements Generatable {
  protected final PrimitiveType[] leftTypes;
  protected final PrimitiveType[] rightTypes;
  protected final int keyPositionLeft;
  protected final int keyPositionRight;
  protected final int windowLength;

}