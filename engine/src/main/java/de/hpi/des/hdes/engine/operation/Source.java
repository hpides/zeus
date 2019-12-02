package de.hpi.des.hdes.engine.operation;

public interface Source<OUT> extends Initializable<OUT> {

  void read();
}
