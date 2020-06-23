package de.hpi.des.hdes.engine.generators;

import lombok.Getter;

@Getter
public enum PrimitiveType {
    INT(Integer.BYTES, "int", "Int"), LONG(Long.BYTES, "long", "Long");

    private final int length;
    private final String lowercaseName;
    private final String uppercaseName;

    private PrimitiveType(final int length, final String lowercaseName, final String uppercaseName) {
        this.length = length;
        this.lowercaseName = lowercaseName;
        this.uppercaseName = uppercaseName;
    }
}