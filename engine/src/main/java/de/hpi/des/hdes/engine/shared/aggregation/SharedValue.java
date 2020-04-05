package de.hpi.des.hdes.engine.shared.aggregation;

import lombok.Value;

@Value
public class SharedValue<V> {
  V value;
  QuerySet querySet;
}
