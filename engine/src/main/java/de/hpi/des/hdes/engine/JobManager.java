package de.hpi.des.hdes.engine;


import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobManager {

  private final Engine engine;
  private final Timer timer;

  public JobManager() {
    this.engine = new Engine();
    this.timer = new Timer("AddQueryTimer");
  }

  public void addQuery(final Query query) {
    this.engine.addQuery(query);
  }

  public void addQuery(final Query query, final long delay, final ChronoUnit timeUnit) {
    final TimerTask addQueryTask = new AddQueryTimerTask(this.engine, query);
    this.timer.schedule(addQueryTask, TimeUnit.of(timeUnit).toMillis(delay));
  }

  public void runEngine() {
    this.engine.run();
  }
}
