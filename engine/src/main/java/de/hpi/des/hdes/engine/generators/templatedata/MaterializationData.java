package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

@Getter
public class MaterializationData {
  final private String varName;
  final private int index;
  final private PrimitiveType type;
  final private boolean fromBuffer;
  final private String inputName;

  public MaterializationData(final int count, final PrimitiveType type, final String inputName) {
    this.varName = "$".concat(Integer.toString(count));
    this.index = 0;
    this.type = type;
    this.fromBuffer = false;
    this.inputName = inputName;
  }

  public MaterializationData(final int count, final int index, final PrimitiveType type, final String inputName) {
    this.varName = "$".concat(Integer.toString(count));
    this.index = index;
    this.type = type;
    this.fromBuffer = true;
    this.inputName = inputName;
  }
}