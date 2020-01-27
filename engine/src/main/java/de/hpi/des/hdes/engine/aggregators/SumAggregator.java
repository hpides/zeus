package de.hpi.des.hdes.engine.aggregators;

import de.hpi.des.hdes.engine.udf.Aggregator;

public class SumAggregator implements Aggregator<Integer, Integer, Integer> {

    @Override
    public Integer initialize() {
        return 0;
    }

    @Override
    public Integer add(final Integer state, final Integer input) {
        return state + input;
    }

    @Override
    public Integer getResult(final Integer state) {
        return state;
    }
}
