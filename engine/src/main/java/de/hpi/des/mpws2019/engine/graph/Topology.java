package de.hpi.des.mpws2019.engine.graph;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

// topology
@RequiredArgsConstructor
public class Topology {

  @Getter
  private final List<SourceNode<?>> sourceNodes;
}
