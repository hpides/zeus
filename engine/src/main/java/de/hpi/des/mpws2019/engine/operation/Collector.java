package de.hpi.des.mpws2019.engine.operation;

public interface Collector<T> {

  void collect(T t);
}
