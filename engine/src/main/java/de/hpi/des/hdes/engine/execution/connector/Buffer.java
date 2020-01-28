package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.AData;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public interface Buffer<IN> {

  /**
   * @return The next value from the buffer or null
   */
  @Nullable
  AData<IN> poll();


  /**
   * @return All the values currently in the buffer. This might, however, not be thread save and
   * should only be used with cation in testing.
   */
  List<AData<IN>> unsafePollAll();

  void add(AData<IN> val);

  default void flush(){}

  default void flushIfTimeout(){}

  static <IN> Buffer<IN> create(){
    return new ChunkedBuffer<>();
  }
}
