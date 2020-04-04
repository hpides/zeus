package de.hpi.des.hdes.engine.shared.join;

import static org.jooq.lambda.Seq.seq;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

@Slf4j
public class StreamAJoin<IN1, IN2, KEY> extends AbstractTopologyElement<IntersectedBucket<IN1, IN2>>
    implements TwoInputOperator<Bucket<KEY, IN1>, Bucket<KEY, IN2>, IntersectedBucket<IN1, IN2>> {

  // perform set intersection between the index entries of input buckets
  // late materialization --> avoid performing the cross-product of intersection
  private final Map<Window, List<Bucket<KEY, IN1>>> state1 = new HashMap<>();
  private final Map<Window, List<Bucket<KEY, IN2>>> state2 = new HashMap<>();
  private final WindowAssigner<? extends Window> windowAssigner;

  public StreamAJoin(final WindowAssigner<? extends Window> windowAssigner) {
    this.windowAssigner = windowAssigner;
  }

  @Override
  public void processStream1(final AData<Bucket<KEY, IN1>> aData) {
    final List<? extends Window> assignedWindows = this.windowAssigner
        .assignWindows(aData.getEventTime());

    for (final Window window : assignedWindows) {
      final List<Bucket<KEY, IN1>> windowState = this.state1
          .computeIfAbsent(window, w -> new ArrayList<>());
      windowState.add(aData.getValue());
    }

    if (aData.isWatermark()) {
      final long timestamp = ((ADataWatermark<?>) aData).getWatermarkTimestamp();
      this.trigger(timestamp);
      final long nextWindowStart = this.windowAssigner.nextWindowStart(timestamp);
      this.state2.entrySet().removeIf(entry -> entry.getKey().getMaxTimestamp() < nextWindowStart);
    }
  }

  @Override
  public void processStream2(final AData<Bucket<KEY, IN2>> aData) {
    final List<? extends Window> assignedWindows = this.windowAssigner
        .assignWindows(aData.getEventTime());

    for (final Window window : assignedWindows) {
      final List<Bucket<KEY, IN2>> windowState = this.state2
          .computeIfAbsent(window, w -> new ArrayList<>());
      windowState.add(aData.getValue());
    }

    if (aData.isWatermark()) {
      final long timestamp = ((ADataWatermark<?>) aData).getWatermarkTimestamp();
      this.trigger(timestamp);
      final long nextWindowStart = this.windowAssigner.nextWindowStart(timestamp);
      this.state1.entrySet().removeIf(entry -> entry.getKey().getMaxTimestamp() < nextWindowStart);
    }
  }

  private void trigger(final long timestamp) {
    for (final Entry<Window, List<Bucket<KEY, IN1>>> entry : this.state1.entrySet()) {
      final Window window = entry.getKey();
      // not closed
      if (window.getMaxTimestamp() > timestamp) {
        continue;
      }

      final List<Bucket<KEY, IN2>> otherBuckets = this.state2.get(window);

      // no join partner available
      if (otherBuckets == null) {
        continue;
      }

      final Collection<IntersectedBucket<IN1, IN2>> intersectedBuckets = this.buildIntersections(
          entry.getValue(), otherBuckets);
      intersectedBuckets.forEach(bucket -> this.collector.collect(AData.of(bucket)));
    }
  }

  private Collection<IntersectedBucket<IN1, IN2>> buildIntersections(
      final Collection<Bucket<KEY, IN1>> in1Buckets,
      final Collection<Bucket<KEY, IN2>> in2Buckets) {

    return seq(in1Buckets)
        .crossJoin(in2Buckets)
        .flatMap(this::getMergesEntries)
        .toList();
  }

  private Seq<IntersectedBucket<IN1, IN2>> getMergesEntries(
      final Tuple2<Bucket<KEY, IN1>, Bucket<KEY, IN2>> bucketTuple) {
    final Set<KEY> inKeySet1 = bucketTuple.v1.getSet().keySet();
    final Set<KEY> inKeySet2 = bucketTuple.v2.getSet().keySet();
    final Set<KEY> index = Sets.intersection(inKeySet1, inKeySet2);
    return this.mergeEntries(index, bucketTuple.v1.getSet(), bucketTuple.v2.getSet());
  }

  private Seq<IntersectedBucket<IN1, IN2>> mergeEntries(final Set<KEY> index,
      final Map<KEY, ? extends Set<IN1>> entries1, final Map<KEY, ? extends Set<IN2>> entries2) {
    return seq(index).map(i -> new IntersectedBucket<>(entries1.get(i), entries2.get(i)));
  }
}
