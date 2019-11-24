package de.hpi.des.mpws2019.engine.operation;

public interface Output<T> {

  void collect(T t);
}
