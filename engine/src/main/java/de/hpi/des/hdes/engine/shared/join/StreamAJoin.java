package de.hpi.des.hdes.engine.shared.join;

import static org.jooq.lambda.Seq.seq;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

/**
 * StreamAJoin performs the set intersection of the index entries of two
 * buckets.
 *
 * @param <IN1> the input type of the left stream
 * @param <IN2> the input type of the right stream
 * @param <KEY> the key type of the join operation
 */
@Slf4j
public class StreamAJoin<IN1, IN2, KEY> extends AbstractTopologyElement<IntersectedBucket<IN1, IN2>>
        implements TwoInputOperator<Bucket<KEY, IN1>, Bucket<KEY, IN2>, IntersectedBucket<IN1, IN2>> {

    private final Map<Window, List<Bucket<KEY, IN1>>> state1 = new HashMap<>();
    private final Map<Window, List<Bucket<KEY, IN2>>> state2 = new HashMap<>();
    private final WindowAssigner<? extends Window> windowAssigner;
    private int joinSize = 0;
    private long latestWatermarkLeft;
    private long latestWatermarkRight;

    /**
     * @param windowAssigner the window assigner of the join window
     */
    public StreamAJoin(final WindowAssigner<? extends Window> windowAssigner) {
        this.windowAssigner = windowAssigner;
    }

    @Override
    public void processStream1(final AData<Bucket<KEY, IN1>> aData) {
        final List<? extends Window> assignedWindows = this.windowAssigner.assignWindows(aData.getEventTime());

        // add bucket two state
        // todo incrementally merging
        for (final Window window : assignedWindows) {
            final List<Bucket<KEY, IN1>> windowState = this.state1.computeIfAbsent(window, w -> new ArrayList<>());
            windowState.add(aData.getValue());
        }

        if (aData.isWatermark()) {
            final long timestamp = ((ADataWatermark<?>) aData).getWatermarkTimestamp();
            latestWatermarkLeft = Math.max(latestWatermarkLeft, timestamp);
            this.trigger(timestamp);
        }
    }

    @Override
    public void processStream2(final AData<Bucket<KEY, IN2>> aData) {
        final List<? extends Window> assignedWindows = this.windowAssigner.assignWindows(aData.getEventTime());

        // add bucket two state
        // todo incrementally merging
        for (final Window window : assignedWindows) {
            final List<Bucket<KEY, IN2>> windowState = this.state2.computeIfAbsent(window, w -> new ArrayList<>());
            windowState.add(aData.getValue());
        }

        if (aData.isWatermark()) {
            final long timestamp = ((ADataWatermark<?>) aData).getWatermarkTimestamp();
            latestWatermarkRight = Math.max(latestWatermarkRight, timestamp);
            this.trigger(timestamp);
        }
    }

    /**
     * Creates intersected buckets for windows closed by the watermark timestamp
     *
     * @param timestamp the timestamp of the watermark
     */
    private void trigger(final long timestamp) {
        List<Window> triggeredWindows = new LinkedList<>();
        for (final Entry<Window, List<Bucket<KEY, IN1>>> entry : this.state1.entrySet()) {
            final Window window = entry.getKey();
            // not closed
            if (window.getMaxTimestamp() > latestWatermarkRight || window.getMaxTimestamp() > latestWatermarkLeft) {
                continue;
            }
            triggeredWindows.add(window);

            final List<Bucket<KEY, IN2>> otherBuckets = this.state2.get(window);

            // no join partner available
            if (otherBuckets == null) {
                continue;
            }
            joinSize = 0;
            final Collection<IntersectedBucket<IN1, IN2>> intersectedBuckets = this.buildIntersections(entry.getValue(),
                    otherBuckets);
            intersectedBuckets.forEach(bucket -> this.collector.collect(AData.of(bucket)));
        }
        triggeredWindows.forEach(window -> {
            state1.remove(window);
            state2.remove(window);
        });

    }

    /**
     * Builds all set intersections of the current window
     *
     * @param in1Buckets buckets created by the first stream
     * @param in2Buckets buckets created by the second stream
     * @return List of IntersectedBuckets
     */
    private Collection<IntersectedBucket<IN1, IN2>> buildIntersections(final Collection<Bucket<KEY, IN1>> in1Buckets,
            final Collection<Bucket<KEY, IN2>> in2Buckets) {

        final List<IntersectedBucket<IN1, IN2>> buckets = new ArrayList<>(in1Buckets.size());

        for (final Bucket<KEY, IN1> in1Bucket : in1Buckets) {
            for (final Bucket<KEY, IN2> in2Bucket : in2Buckets) {
                final Map<KEY, Set<IN1>> in1BucketSet = in1Bucket.getSet();
                final Map<KEY, Set<IN2>> in2BucketSet = in2Bucket.getSet();
                final Set<KEY> index = Sets.intersection(in1BucketSet.keySet(), in2BucketSet.keySet());
                for (final KEY key : index) {
                    joinSize += in1BucketSet.get(key).size() * in2BucketSet.get(key).size();
                    buckets.add(new IntersectedBucket<>(in1BucketSet.get(key), in2BucketSet.get(key)));
                }

            }
        }
        return buckets;
    }

    @Override
    public void close() {
        this.state1.clear();
        this.state2.clear();
    }
}
