package de.hpi.des.mpws2019.engine.execution.slot;

public abstract class Slot {

  public abstract void run();

  public Runnable makeRunnable() {
    return () -> {
      while (true) {
        Slot.this.run();
      }
    };

  }

}
