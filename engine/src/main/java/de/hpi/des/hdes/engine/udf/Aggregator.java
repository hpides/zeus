package de.hpi.des.hdes.engine.udf;

public interface Aggregator<IN, TYPE, OUT> {

    TYPE initialize();

    TYPE add(TYPE state, IN input);

    OUT getResult(TYPE state);
}
