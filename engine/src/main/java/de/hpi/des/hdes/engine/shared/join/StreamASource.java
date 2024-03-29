package de.hpi.des.hdes.engine.shared.join;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.ADataWatermark;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.udf.KeySelector;
import de.hpi.des.hdes.engine.window.Window;
import de.hpi.des.hdes.engine.window.assigner.TumblingWindow;
import de.hpi.des.hdes.engine.window.assigner.WindowAssigner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * StreamASource creates indexed buckets of join elements.
 *
 * Note: This is not a {@link de.hpi.des.hdes.engine.operation.Source} but {@link OneInputOperator}.
 * The name is based on the AJoin definition.
 *
 * @param <IN>  the input type of the stream elements
 * @param <KEY> the key type of the join
 */
@Slf4j
public class StreamASource<IN, KEY> extends AbstractTopologyElement<Bucket<KEY, IN>>
    implements OneInputOperator<IN, Bucket<KEY, IN>> {

  private final WindowAssigner<? extends Window> sliceAssigner;
  private final KeySelector<IN, KEY> keySelector;
  private final Map<Window, Map<KEY, Set<IN>>> state;

  /**
   * @param triggerInterval the interval of the slices
   * @param keySelector     the key selector of the join
   */
  public StreamASource(final long triggerInterval, final KeySelector<IN, KEY> keySelector) {
    this.sliceAssigner = TumblingWindow.ofEventTime(triggerInterval);
    this.keySelector = keySelector;
    this.state = new HashMap<>();
  }

  /**
   * Updates state and triggers on watermark
   *
   * @param aData the input element
   */
  @Override
  public void process(final AData<IN> aData) {
    final List<? extends Window> assignedWindows = this.sliceAssigner
        .assignWindows(aData.getEventTime());
    // put in own state; we know there is only window
    final Window window = assignedWindows.get(0);
    final Map<KEY, Set<IN>> windowState = this.state.computeIfAbsent(window, w -> new HashMap<>());
    final KEY joinKey = this.keySelector.selectKey(aData.getValue());
    final Set<IN> keyState = windowState.computeIfAbsent(joinKey, key -> new HashSet<>());
    keyState.add(aData.getValue());

    if (aData.isWatermark()) {
      final ADataWatermark<IN> watermark = (ADataWatermark<IN>) aData;
      this.trigger(watermark.getWatermarkTimestamp());
    }
  }

  /**
   * Sends windows closed by the watermark downstream
   *
   * For each closed window, this method creates a new bucket.
   * The last bucket contains the watermark that lead to call of the method.
   *
   * @param watermarkTimestamp the timestamp of the watermark
   */
  private void trigger(final long watermarkTimestamp) {
    final List<AData<Bucket<KEY, IN>>> output = new ArrayList<>(this.state.values().size());
    // we are using an iterator to remove state
    final Iterator<Entry<Window, Map<KEY, Set<IN>>>> iterator = this.state.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Window, Map<KEY, Set<IN>>> windowEntry = iterator.next();
      final Window window = windowEntry.getKey();
      // not closed
      if (window.getMaxTimestamp() > watermarkTimestamp) {
        continue;
      }

      // emit all closed windows
      final Bucket<KEY, IN> bucket = new Bucket<>(windowEntry.getValue(), window);
      final AData<Bucket<KEY, IN>> bucketAData = new AData<>(bucket, window.getMaxTimestamp(),
          false);

      output.add(bucketAData);
      iterator.remove();
    }

    // skip if there are no closed windows
    if (output.isEmpty()) {
      return;
    }

    // send watermark for last element
    final int lastIndex = output.size() - 1;
    final AData<Bucket<KEY, IN>> bucketAData = output.get(lastIndex);
    final ADataWatermark<Bucket<KEY, IN>> watermark = ADataWatermark.from(bucketAData, watermarkTimestamp);
    output.set(lastIndex, watermark);
    output.forEach(this.collector::collect);
  }
}

