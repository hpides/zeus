package de.hpi.des.mpws2019.engine.function;

@FunctionalInterface
public interface Join<V, V2, VR> {

  VR join(V first, V2 second);
}
