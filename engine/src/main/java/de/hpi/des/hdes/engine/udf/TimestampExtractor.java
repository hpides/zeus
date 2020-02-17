package de.hpi.des.hdes.engine.udf;

public interface TimestampExtractor<T> {

    static <T> TimestampExtractor<T> currentTimeNS() {
        return (T e) -> System.nanoTime();
    }

    long apply(T elem);
}
