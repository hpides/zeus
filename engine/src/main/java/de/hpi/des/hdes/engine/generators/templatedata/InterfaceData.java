package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
public class InterfaceData {
  @Getter
  @AllArgsConstructor
  private class ArgumentData {
    private final String name;
    private final PrimitiveType type;
    private final boolean last;
  }

  private final String interfaceName;
  private final String returnType;
  private final ArgumentData[] arguments;

  public InterfaceData(final String interfaceName, final String returnType, final PrimitiveType[] arguments) {
    this.interfaceName = interfaceName;
    this.returnType = returnType;
    this.arguments = new ArgumentData[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      this.arguments[i] = new ArgumentData("ifd".concat(Integer.toString(i)), arguments[i], i == arguments.length - 1);
    }
  }
}