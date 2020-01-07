package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.graph.Topology;
import java.util.TimerTask;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AddQueryTimerTask extends TimerTask {
    private final Engine engine;
    private final Topology query;

    @Override
    public void run() {
        this.engine.addQuery(this.query);
    }
}
