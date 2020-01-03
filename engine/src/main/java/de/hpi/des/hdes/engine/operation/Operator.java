package de.hpi.des.hdes.engine.operation;

public interface Operator<OUT> extends Initializable<OUT> {
  void tick();

}
