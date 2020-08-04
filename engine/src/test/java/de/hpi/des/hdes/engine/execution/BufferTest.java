package de.hpi.des.hdes.engine.execution;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.execution.connector.SlotBuffer;
import de.hpi.des.hdes.engine.execution.connector.ChunkedBuffer;
import de.hpi.des.hdes.engine.execution.connector.QueueBuffer;
import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

public class BufferTest {

  @ParameterizedTest
  @ValueSource(classes = { ChunkedBuffer.class, SizedChunkedBuffer.class, QueueBuffer.class })
  void keepsOrderAndPassesAllElements(Class<SlotBuffer<Integer>> clazz)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    SlotBuffer<Integer> buffer = clazz.getDeclaredConstructor().newInstance();
    int max = 100_000;
    Runnable r1 = () -> {
      for (var i = 1; i <= max; i++) {
        buffer.add(i);
      }
      try {
        Thread.sleep(ExecutionConfig.getConfig().getFlushIntervallMS());
        buffer.flush();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    };
    Runnable r2 = () -> {
      var i = 0;
      var last = 0;
      while (i < max) {
        var res = buffer.poll();
        if (res != null) {
          last = i;
          i = res;
          assertThat(i - 1).isEqualTo(last);
        }
      }
    };
    awaitCompletion(r1, r2);

  }

  @ParameterizedTest
  @ValueSource(classes = { ChunkedBuffer.class, SizedChunkedBuffer.class, QueueBuffer.class })
  void flushesBufferOnTimeout(Class<SlotBuffer<Integer>> clazz)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    SlotBuffer<Integer> buffer = clazz.getDeclaredConstructor().newInstance();
    Runnable r1 = () -> {
      for (var i = 1; i < ExecutionConfig.getConfig().getChunkSize(); i++) {
        buffer.add(i);
      }
      try {
        Thread.sleep(ExecutionConfig.getConfig().getFlushIntervallMS());
        buffer.flushIfTimeout();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    };
    Runnable r2 = () -> {
      var i = 0;
      var last = 0;
      while (i < ExecutionConfig.getConfig().getChunkSize() - 1) {
        var res = buffer.poll();
        if (res != null) {
          last = i;
          i = res;
          assertThat(i - 1).isEqualTo(last);
        }
      }
    };
    awaitCompletion(r1, r2);
  }

  /**
   * Because we assert in a Callable object we need to check for potential
   * assertion in its future's results. As exceptions in threads are not
   * immediately passed to the parent thread.
   *
   */
  private void awaitCompletion(Runnable r1, Runnable r2) {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    var f1 = executor.submit(r1);
    var f2 = executor.submit(r2);
    try {
      f1.get();
      f2.get();
    } catch (ExecutionException ae) {
      if (ae.getCause() instanceof AssertionFailedError) {
        throw (AssertionFailedError) ae.getCause();
      }
      ae.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
