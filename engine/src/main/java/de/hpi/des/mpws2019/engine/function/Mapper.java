package de.hpi.des.mpws2019.engine.function;

public interface Mapper<V, R> {

  R map(V in);
}
