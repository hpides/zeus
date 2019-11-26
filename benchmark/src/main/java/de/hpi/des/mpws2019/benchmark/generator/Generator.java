package de.hpi.des.mpws2019.benchmark.generator;

import de.hpi.des.mpws2019.benchmark.Event;
import de.hpi.des.mpws2019.benchmark.TimedBlockingSource;
import de.hpi.des.mpws2019.engine.operation.Source;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public interface Generator<E extends Event> {

  /**
   * Generates events and puts them into the queue.
   * @param queue This queue receives the generated events.
   * @return Future is completed with true or false depending on the success of the generation.
   */
  CompletableFuture<Boolean> generate(TimedBlockingSource<E> queue);

  Long getTotalEvents();

}
