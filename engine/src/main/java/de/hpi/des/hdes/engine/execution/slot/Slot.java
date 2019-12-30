package de.hpi.des.hdes.engine.execution.slot;

import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class Slot implements Runnable {

  @Getter
  private final UUID topologyNodeId;
  @Getter
  @Setter
  private boolean alreadyRunning = false;

  public abstract void runStep();

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      this.runStep();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if(obj.getClass().equals(this.getClass())) {
      log.error("Got unexpected object tp compare to");
      throw new IllegalArgumentException();
    }
    Slot slotObj = (Slot) obj;
    return this.getTopologyNodeId() == slotObj.getTopologyNodeId();
  }

}
