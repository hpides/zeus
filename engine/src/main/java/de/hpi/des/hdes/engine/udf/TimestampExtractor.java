package de.hpi.des.hdes.engine.udf;

public interface TimestampExtractor<T> {
    public long apply(T elem);
}
