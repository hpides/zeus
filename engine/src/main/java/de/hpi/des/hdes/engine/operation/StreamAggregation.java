package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.List;

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

    public void closeOutdatedWindows(final List<? extends Window> activeWindows) {
        for(final Window window : this.windowToState.keySet()) {
            if(!activeWindows.contains(window)) {
                this.collector.collect(AData.of(this.aggregator.getResult(this.windowToState.get(window))));
                this.windowToState.remove(window);
            }
        }
    }

    @Override
    public void process(final AData<IN> aData) {
        final List<? extends Window> activeWindows = this.windowAssigner.assignWindows(aData.getEventTime());
        this.closeOutdatedWindows(activeWindows);
        final IN input = aData.getValue();
        // Add the input to the windows it belongs to
        for (final Window window : activeWindows) {
            final STATE state = this.windowToState.computeIfAbsent(window, w -> this.aggregator.initialize());
            this.windowToState.put(window, this.aggregator.add(state, input));
        }
    }
}
