package de.hpi.des.mpws2019.engine.execution.slot;

public interface InputBuffer<IN> {

  IN poll();
}
