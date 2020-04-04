package de.hpi.des.hdes.engine.shared.join;

import java.util.Set;
import lombok.Value;

/**
 * A class with two sets of values.
 *
 * This class is part of AJoin.
 * An {@code IntersectedBucket} contains only values that have the same key.
 *
 * @param <VAL1> the type of the values of the first stream
 * @param <VAL2> te type of the values of the second stream
 */
@Value
public class IntersectedBucket<VAL1, VAL2> {

  private Set<VAL1> v1;
  private Set<VAL2> v2;
}
