package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.HashMap;
import java.util.List;

public class StreamAggregation<IN, TYPE, OUT, W extends Window> extends AbstractInitializable<OUT> implements OneInputOperator<IN, OUT> {
    private final Aggregator<IN, TYPE, OUT> aggregator;
    private final WindowAssigner<W> windowAssigner;
    private final HashMap<W, TYPE> windowToState;

    public StreamAggregation(final Aggregator<IN, TYPE, OUT> aggregator, final WindowAssigner<W> windowAssigner) {
        this.aggregator = aggregator;
        this.windowAssigner = windowAssigner;
        this.windowToState = new HashMap<>();
        aggregator.initialize();
    }

    public void update() {
        // Send the finished windows to the collector
        // A Windows is considered finished when a new element is no longer assigned to it
        // Windows are only sent out if they are finished
        final List<W> assignedWindows = this.windowAssigner.assignWindows(System.currentTimeMillis());
        for(final W window : windowToState.keySet()) {
            if(!assignedWindows.contains(window)) {
                collector.collect(aggregator.getResult(windowToState.get(window)));
            }
        }
    }

    @Override
    public void process(IN input) {
        update();
        final List<W> assignedWindows = this.windowAssigner.assignWindows(System.currentTimeMillis());

        // Add the input to the windows it belongs to
        for (final W window : assignedWindows) {
            if(windowToState.containsKey(window)) {
                windowToState.put(window, aggregator.add(windowToState.get(window), input));
            }
            else {
                TYPE state = aggregator.initialize();
                windowToState.put(window, aggregator.add(state, input));
            }
        }
    }
}
