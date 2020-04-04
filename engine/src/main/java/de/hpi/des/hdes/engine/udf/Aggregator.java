package de.hpi.des.hdes.engine.udf;

/**
 * Interface for aggregators.
 *
 * @param <IN> type of the input elements
 * @param <STATE> type of the intermediate state
 * @param <OUT> type of the output elements
 */
public interface Aggregator<IN, STATE, OUT> {

    /**
     * Initializes the aggregator.
     *
     * @return the initial state of type STATE
     */
    STATE initialize();

    /**
     * Adds a new element to the intermediate state.
     *
     * @param state the intermediate state
     * @param input new input element
     * @return new state with new input element
     */
    STATE add(STATE state, IN input);

    /**
     * Transforms the intermediate state to an element of the output type.
     *
     * @param state intermediate state object.
     * @return the transformed intermediate state
     */
    OUT getResult(STATE state);
}
