package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.graph.Topology;
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

    public void addQuery(Topology topology) {
        this.engine.addQuery(topology);
    }

    public void addQuery(Topology topology, long delay, ChronoUnit timeUnit) {
        TimerTask addQueryTask = new AddQueryTimerTask(engine, topology);
        this.timer.schedule(addQueryTask, TimeUnit.of(timeUnit).toMillis(delay));
    }

    public void runEngine() {
        this.engine.run();
    }
}
