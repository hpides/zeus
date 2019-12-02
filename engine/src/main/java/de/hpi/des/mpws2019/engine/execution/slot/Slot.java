package de.hpi.des.mpws2019.engine.execution.slot;

public abstract class Slot implements Runnable {

  public abstract void runStep();

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      this.runStep();
    }
  }

}
