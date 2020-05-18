package de.hpi.des.hdes.engine;

import java.util.TimerTask;
import lombok.AllArgsConstructor;

/**
 * Utility to delay the adding of queries
 */
@AllArgsConstructor
public class AddQueryTimerTask extends TimerTask {

  private final VulcanoEngine engine;
  private final Query query;

  @Override
  public void run() {
    this.engine.addQuery(this.query);
  }
}
