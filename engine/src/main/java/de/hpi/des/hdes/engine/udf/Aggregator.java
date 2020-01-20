package de.hpi.des.hdes.engine.udf;

public interface Aggregator<IN, STATE, OUT> {

    STATE initialize();

    STATE add(STATE state, IN input);

    OUT getResult(STATE state);
}
