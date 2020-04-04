package de.hpi.des.hdes.engine.udf;

/**
 * Extracts a timestamp from a generic object.
 */
public interface TimestampExtractor<T> {

    /**
     * Factory method.
     *
     * @return timestamp extractor
     */
    static <T> TimestampExtractor<T> currentTimeNS() {
        return (T e) -> System.nanoTime();
    }

    /**
     * Returns the extracted timestamp from the input element.
     *
     * @param elem input element
     * @return extracted timestamp
     */
    long apply(T elem);
}
