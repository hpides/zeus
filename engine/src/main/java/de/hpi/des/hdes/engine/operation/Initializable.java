package de.hpi.des.hdes.engine.operation;

public interface Initializable<OUT> {

  void init(Collector<OUT> collector);
}
