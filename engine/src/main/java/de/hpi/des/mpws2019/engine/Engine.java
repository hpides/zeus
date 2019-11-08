package de.hpi.des.mpws2019.engine;

import de.hpi.des.mpws2019.engine.sink.Sink;
import de.hpi.des.mpws2019.engine.source.Source;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Engine<K> implements Runnable {
    private volatile boolean shutdown;
    private final Source<K> source;
    private final Sink<K> sink;
    private final Function<K, K> map;

    @Override
    public void run() {
        while (!this.shutdown) {
            final K input = this.source.poll();
            if (input != null) {
                final K result = this.map.apply(input);
                this.sink.write(result);
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
