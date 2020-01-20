package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class StreamAggregation<IN, STATE, OUT> extends AbstractTopologyElement<OUT> implements OneInputOperator<IN, OUT> {
    private final Aggregator<IN, STATE, OUT> aggregator;
    private final WindowAssigner<? extends Window> windowAssigner;
    private final HashMap<Window, STATE> windowToState;

    public StreamAggregation(final Aggregator<IN, STATE, OUT> aggregator, final WindowAssigner<? extends Window> windowAssigner) {
        this.aggregator = aggregator;
        this.windowAssigner = windowAssigner;
        this.windowToState = new HashMap<>();
        aggregator.initialize();
    }

    public void closeOutdatedWindows(List<? extends Window> activeWindows) {
        for(final Window window : windowToState.keySet()) {
            if(!activeWindows.contains(window)) {
                collector.collect(aggregator.getResult(windowToState.get(window)));
                windowToState.remove(window);
            }
        }
    }

    @Override
    public void process(@NotNull IN input) {
        final List<? extends Window> activeWindows = this.windowAssigner.assignWindows(System.nanoTime());
        closeOutdatedWindows(activeWindows);

        // Add the input to the windows it belongs to
        for (final Window window : activeWindows) {
            STATE state = windowToState.computeIfAbsent(window, w -> aggregator.initialize());
            windowToState.put(window, aggregator.add(state, input));
        }
    }
}
