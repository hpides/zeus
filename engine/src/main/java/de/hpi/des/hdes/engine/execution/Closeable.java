package de.hpi.des.hdes.engine.execution;

public interface Closeable {
  default void close() {
    // do nothing
  }
}
