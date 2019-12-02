package de.hpi.des.hdes.engine.operation;

public interface Collector<T> {

  void collect(T t);
}
