package de.hpi.des.hdes.engine.aggregators;

import de.hpi.des.hdes.engine.udf.Aggregator;

public class SumAggregator implements Aggregator<Integer, Integer, Integer> {

    @Override
    public Integer initialize() {
        return 0;
    }

    @Override
    public Integer add(Integer state, Integer input) {
        return state + input;
    }

    @Override
    public Integer getResult(Integer state) {
        return state;
    }
}
