package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.Query;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RunnableSlot<OUT> extends Slot<OUT> implements Runnable {

  private boolean running = false;
  private volatile boolean shutdownFlag = false;

  /**
   * runStep should never block indefinitely as outgoing buffers might not have been flushed yet.
   */
  public abstract void runStep();

  public void shutdown() {
    this.shutdownFlag = true;
  }

  @Override
  public void run() {
    this.running = true;
    while (!Thread.currentThread().isInterrupted() || this.shutdownFlag) {
      this.runStep();
      this.tick();
    }
  }

  @Override
  public void remove(final Query query) {
    super.remove(query);
    // if there are no downstream processor left, we can shutdown the slot
    if (this.getOutputs().stream().allMatch(Map::isEmpty)) {
      log.debug("Shutdown slot {}", this);
      this.shutdown();
    }
  }

  public boolean isShutdown() {
    return this.shutdownFlag;
  }

  public boolean isRunning() {
    return this.running;
  }

}
