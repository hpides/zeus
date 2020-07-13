package de.hpi.des.hdes.engine.generators.templatedata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class MapData {
  final private String varName;
  final private String signature;
  final private String functionBody;
  final private String application;
  final private String interfaceName;
}