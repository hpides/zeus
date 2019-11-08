package de.hpi.des.mpws2019.engine.sink;

public interface Sink<V> {
    void write(V input);
}
