package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.Query;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class VulcanoRunnableSlot<OUT> extends Slot<OUT> implements Runnable {

  private boolean enabled = false;
  private volatile boolean shutdownFlag = false;

  /**
   * runStep should never block indefinitely as outgoing buffers might not have
   * been flushed yet.
   */
  public abstract void runStep();

  public void shutdown() {
    this.shutdownFlag = true;
  }

  @Override
  public void run() {
    this.enabled = true;
    try {
      while (!Thread.currentThread().isInterrupted() && !this.shutdownFlag) {
        this.runStep();
        this.tick();
      }
      log.debug("Stopped running {}", this);
    } catch (final RuntimeException e) {
      log.error("Slot had an exception: ", e);
      throw e;
    } finally {
      this.enabled = false;
      this.cleanUp();
    }
  }

  @Override
  public void remove(final Query query) {
    super.remove(query);
    // if there are no downstream processor left, we can shutdown the slot
    if (this.hasNoOutput()) {
      log.debug("Shutdown slot {}", this);
      this.shutdown();
    }
  }

  @Override
  public boolean isShutdown() {
    return this.shutdownFlag;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

}
