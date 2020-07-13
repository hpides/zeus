package de.hpi.des.hdes.engine.generators.templatedata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SourceData {
  private String className;
  private String nextPipelineClass;
  private String nextPipelineFunction;
}