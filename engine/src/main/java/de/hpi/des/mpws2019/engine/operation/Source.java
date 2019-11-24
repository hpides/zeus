package de.hpi.des.mpws2019.engine.operation;

public interface Source<OUT> extends Initializable<OUT> {

  void read();
}
