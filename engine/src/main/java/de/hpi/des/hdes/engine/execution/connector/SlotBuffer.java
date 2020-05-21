package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.Closeable;
import de.hpi.des.hdes.engine.execution.SlotProcessor;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * A Buffer is used to connect two runnable slots.
 *
 * @param <IN> type of elements to buffer
 */
public interface SlotBuffer<IN> extends SlotProcessor<IN>, Closeable {

  static <IN> SlotBuffer<AData<IN>> createADataBuffer() {
    return new BlockingSizedChunkedBuffer<>(250_000);
  }

  static <IN> SlotBuffer<IN> create() {
    return new ChunkedBuffer<>();
  }

  /**
   * @return The next value from the buffer or null
   */
  @Nullable
  IN poll();

  /**
   * @return All the values currently in the buffer. This might, however, not be
   *         thread save and should only be used with cation in testing.
   */
  List<IN> unsafePollAll();

  default void flush() {
  }

  default void flushIfTimeout() {
  }

  void add(IN val);

  @Override
  default void sendDownstream(IN event) {
    this.add(event);
  }

}
