package de.hpi.des.hdes.benchmark;

public abstract class AbstractSerializer<T> {

  public abstract String serialize(T obj);

  public abstract T deserialize(String obj);
}
