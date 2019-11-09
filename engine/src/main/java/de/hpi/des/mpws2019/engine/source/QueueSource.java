package de.hpi.des.mpws2019.engine.source;

import java.util.Queue;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueueSource<V> implements Source<V> {

  private final Queue<V> queue;

  @Override
  public V poll() {
    return this.queue.poll();
  }
}
