package de.hpi.des.mpws2019.engine.operation;

public interface Initializable<OUT> {

  void init(Collector<OUT> collector);
}