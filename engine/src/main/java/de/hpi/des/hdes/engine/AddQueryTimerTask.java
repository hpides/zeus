package de.hpi.des.hdes.engine;

import java.util.TimerTask;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AddQueryTimerTask extends TimerTask {

  private final Engine engine;
  private final Query query;

  @Override
  public void run() {
    this.engine.addQuery(this.query);
  }
}
