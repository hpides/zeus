package de.hpi.des.hdes.engine;

import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * The job manager is a utility class to mock ad-hoc queries.
 */
@Slf4j
public class JobManager {

    private final Engine engine;
    private final Timer timer;

    public JobManager(Engine engine) {
        this.engine = engine;
        this.timer = new Timer("AddQueryTimer");
    }

    public void addQuery(final Query query) {
        this.engine.addQuery(query);
    }

    public void deleteQuery(final Query query) {
        this.engine.deleteQuery(query);
    }

    public void deleteQuery(final Query query, final long delay, final ChronoUnit timeUnit) {
        final TimerTask addQueryTask = new DeleteQueryTimerTask(this.engine, query);
        this.timer.schedule(addQueryTask, TimeUnit.of(timeUnit).toMillis(delay));
    }

    public void addQuery(final Query query, final long delay, final ChronoUnit timeUnit) {
        final TimerTask addQueryTask = new AddQueryTimerTask(this.engine, query);
        this.timer.schedule(addQueryTask, TimeUnit.of(timeUnit).toMillis(delay));
    }

    public void shutdown() {
        this.engine.shutdown();
        this.timer.cancel();
    }
}
