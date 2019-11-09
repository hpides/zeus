package de.hpi.des.mpws2019.engine.sink;

import java.util.ArrayList;
import java.util.Collection;
import lombok.Getter;

public class CollectionSink<V> implements Sink<V> {

  @Getter
  private final Collection<V> collection = new ArrayList<>();

  @Override
  public void write(final V input) {
    this.collection.add(input);
  }
}
