package de.hpi.des.mpws2019.engine.source;

public interface Source<K> {

  K poll();
}
