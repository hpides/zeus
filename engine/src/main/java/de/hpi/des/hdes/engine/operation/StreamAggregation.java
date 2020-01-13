package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class StreamAggregation<IN, STATE, OUT, W extends Window> extends AbstractTopologyElement<OUT> implements OneInputOperator<IN, OUT> {
    private final Aggregator<IN, STATE, OUT> aggregator;
    private final WindowAssigner<W> windowAssigner;
    private final HashMap<W, STATE> windowToState;

    public StreamAggregation(final Aggregator<IN, STATE, OUT> aggregator, final WindowAssigner<W> windowAssigner) {
        this.aggregator = aggregator;
        this.windowAssigner = windowAssigner;
        this.windowToState = new HashMap<>();
        aggregator.initialize();
    }

    public void closeOutdatedWindows(List<W> activeWindows) {
        for(final W window : windowToState.keySet()) {
            if(!activeWindows.contains(window)) {
                collector.collect(aggregator.getResult(windowToState.get(window)));
                windowToState.remove(window);
            }
        }
    }

    @Override
    public void process(@NotNull IN input) {
        final List<W> activeWindows = this.windowAssigner.assignWindows(System.nanoTime());
        closeOutdatedWindows(activeWindows);

        // Add the input to the windows it belongs to
        for (final W window : activeWindows) {
            STATE state = windowToState.computeIfAbsent(window, w -> aggregator.initialize());
            windowToState.put(window, aggregator.add(state, input));
        }
    }
}
