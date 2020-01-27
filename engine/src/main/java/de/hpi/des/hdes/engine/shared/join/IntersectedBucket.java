package de.hpi.des.hdes.engine.shared.join;

import java.util.Set;
import lombok.Value;

@Value
public class IntersectedBucket<VAL1, VAL2> {

  private Set<VAL1> v1;
  private Set<VAL2> v2;
}
