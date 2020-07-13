package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

@Getter
public class MaterializationData {
  final private String varName;
  final private int index;
  final private PrimitiveType type;
  final private boolean fromBuffer;

  public MaterializationData(final int count, final PrimitiveType type) {
    this.varName = "$".concat(Integer.toString(count));
    this.index = 0;
    this.type = type;
    this.fromBuffer = false;
  }

  public MaterializationData(final int count, final int index, final PrimitiveType type) {
    this.varName = "$".concat(Integer.toString(count));
    this.index = index;
    this.type = type;
    this.fromBuffer = true;
  }
}