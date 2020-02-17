package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.AData;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public interface Buffer<IN> {

  static <IN> Buffer<AData<IN>> createADataBuffer() {
    return new ChunkedBuffer<>();
  }

  static <IN> Buffer<IN> create() {
    return new ChunkedBuffer<>();
  }

  /**
   * @return The next value from the buffer or null
   */
  @Nullable
  IN poll();

  /**
   * @return All the values currently in the buffer. This might, however, not be thread save and
   * should only be used with cation in testing.
   */
  List<IN> unsafePollAll();

  default void flush() {
  }

  default void flushIfTimeout() {
  }

  void add(IN val);


}
