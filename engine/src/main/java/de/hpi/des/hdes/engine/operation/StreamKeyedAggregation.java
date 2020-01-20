package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StreamKeyedAggregation<IN, KEY, STATE, OUT>
        extends AbstractTopologyElement<OUT>
        implements OneInputOperator<IN, OUT> {

    private final Aggregator<IN, STATE, OUT> aggregator;
    private final WindowAssigner<Window> windowAssigner;
    private final HashMap<Window, HashMap<KEY, STATE>> windowToState;
    private final KeySelector<IN, KEY> keyselector;

    public StreamKeyedAggregation(KeySelector<IN, KEY> keyselector,
                                  final Aggregator<IN, STATE, OUT> aggregator,
                                  final WindowAssigner<Window> windowAssigner) {
        this.keyselector = keyselector;
        this.aggregator = aggregator;
        this.windowAssigner = windowAssigner;
        this.windowToState = new HashMap<>();
        aggregator.initialize();
    }

    public void closeOutdatedWindows(List<Window> activeWindows) {
        for(final Window window : windowToState.keySet()) {
            if(!activeWindows.contains(window)) {
                Map<KEY, STATE> keyToState = windowToState.get(window);
                for (KEY key : keyToState.keySet()) {
                    collector.collect(aggregator.getResult(keyToState.get(key)));
                }
                windowToState.remove(window);
            }
        }
    }

    @Override
    public void process(@NotNull IN input) {
        final List<Window> activeWindows = this.windowAssigner.assignWindows(System.nanoTime());
        closeOutdatedWindows(activeWindows);
        KEY key = keyselector.selectKey(input);

        // Add the input to the windows it belongs to
        for (final Window window : activeWindows) {
            Map<KEY, STATE> inputKeyToState = windowToState.computeIfAbsent(window, w -> new HashMap<>());
            STATE state = inputKeyToState.computeIfAbsent(key, k -> aggregator.initialize());
            inputKeyToState.put(key, aggregator.add(state, input));
        }
    }
}
