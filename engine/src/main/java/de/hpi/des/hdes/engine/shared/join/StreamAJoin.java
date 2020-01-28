package de.hpi.des.hdes.engine.shared.join;

import static org.jooq.lambda.Seq.seq;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.window.Window;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamAJoin<IN1, IN2, KEY> extends AbstractTopologyElement<IntersectedBucket<IN1, IN2>>
    implements TwoInputOperator<Bucket<KEY, IN1>, Bucket<KEY, IN2>, IntersectedBucket<IN1, IN2>> {

  // perform set intersection between the index entries of input buckets
  // late materialization --> avoid performing the cross-product of intersection
  private final Map<Window, List<Bucket<KEY, IN1>>> state1 = new HashMap<>();
  private final Map<Window, List<Bucket<KEY, IN2>>> state2 = new HashMap<>();

  @Override
  public void processStream1(final AData<Bucket<KEY, IN1>> aData) {
    final var inBucket = aData.getValue();
    if (this.state2.containsKey(inBucket.getWindow())) {
      final List<Bucket<KEY, IN2>> buckets = this.state2.get(inBucket.getWindow());
      for (final Bucket<KEY, IN2> stateBucket : buckets) {
        final Collection<IntersectedBucket<IN1, IN2>> intersectedBuckets =
            this.buildIntersections(inBucket, stateBucket);
        intersectedBuckets.forEach(b -> this.collector.collect(AData.of(b)));
      }
      // todo remove with watermark
      this.state2.remove(inBucket.getWindow());
    }
    final List<Bucket<KEY, IN1>> bucket = this.state1.computeIfAbsent(inBucket.getWindow(),
        w -> new ArrayList<>());
    bucket.add(inBucket);
  }

  @Override
  public void processStream2(final AData<Bucket<KEY, IN2>> aData) {
    final var inBucket = aData.getValue();
    if (this.state1.containsKey(inBucket.getWindow())) {
      final List<Bucket<KEY, IN1>> buckets = this.state1.get(inBucket.getWindow());
      for (final Bucket<KEY, IN1> stateBucket : buckets) {
        final Collection<IntersectedBucket<IN1, IN2>> intersectedBuckets =
            this.buildIntersections(stateBucket, inBucket);
        intersectedBuckets.forEach(b -> this.collector.collect(AData.of(b)));
      }
      // todo remove with watermark
      this.state1.remove(inBucket.getWindow());
    }
    final List<Bucket<KEY, IN2>> bucket = this.state2.computeIfAbsent(inBucket.getWindow(),
        w -> new ArrayList<>());
    bucket.add(inBucket);
  }

  private Collection<IntersectedBucket<IN1, IN2>> buildIntersections(
      final Bucket<KEY, IN1> in1Bucket, final Bucket<KEY, IN2> in2Bucket) {
    final Set<KEY> inKeySet1 = in1Bucket.getSet().keySet();
    final Set<KEY> inKeySet2 = in2Bucket.getSet().keySet();
    final Set<KEY> index = Sets.intersection(inKeySet1, inKeySet2);
    return this.mergeEntries(index, in1Bucket.getSet(), in2Bucket.getSet());
  }

  private Collection<IntersectedBucket<IN1, IN2>> mergeEntries(final Set<KEY> index,
      final Map<KEY, ? extends Set<IN1>> entries1, final Map<KEY, ? extends Set<IN2>> entries2) {
    return seq(index)
        .map(i -> new IntersectedBucket<>(entries1.get(i), entries2.get(i)))
        .toList();
  }

}
