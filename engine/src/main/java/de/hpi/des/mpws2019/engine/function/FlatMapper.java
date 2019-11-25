package de.hpi.des.mpws2019.engine.function;

public interface FlatMapper<V, R> {

  Iterable<R> flatMap(V in);
}
