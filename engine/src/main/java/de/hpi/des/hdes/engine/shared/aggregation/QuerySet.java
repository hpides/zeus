package de.hpi.des.hdes.engine.shared.aggregation;

import java.util.BitSet;

public class QuerySet extends BitSet {

  public QuerySet() {
  }

  public QuerySet(final int nbits) {
    super(nbits);
  }

  @Override
  public QuerySet clone() {
    return (QuerySet) super.clone();
  }
}
