package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.BlockingOffer;
import java.util.concurrent.CompletableFuture;

public interface Generator<E> {

  /**
   * Generates events and puts them into the queue.
   *
   * @param queue This queue receives the generated events.
   * @return Future is completed with true or false depending on the success of the generation.
   */
  CompletableFuture<Boolean> generate(BlockingOffer<E> queue);

  Long getTotalEvents();

  void shutdown();

}
