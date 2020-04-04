package de.hpi.des.hdes.engine;

import java.util.TimerTask;
import lombok.AllArgsConstructor;

/**
 * Utility to delay the deletion of queries.
 */
@AllArgsConstructor
public class DeleteQueryTimerTask extends TimerTask {

  private final Engine engine;
  private final Query query;

  @Override
  public void run() {
    this.engine.deleteQuery(this.query);
  }
}
