package de.hpi.des.mpws2019.engine.operation;

public interface Sink<IN> extends OneInputOperator<IN, IN> {
  Collector<IN> getCollector();
}

