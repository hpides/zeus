package de.hpi.des.hdes.engine.operation;

import de.hpi.des.hdes.engine.AData;

/**
 * The collector interface is implemented by classes that handle operate as downstream elements.
 *
 * @param <T> type of the elements to collect
 */
public interface Collector<T> {

  void collect(AData<T> t);

  /**
   * This method passes a heartbeat through the pipeline. This can be used if work has to be done
   * even though no new events arrived. E.g., a buffer should time out.
   * This tick should be passed to the connected collectors. The default implementation, however,
   * does nothing for convenience reasons, as for example sinks might not need ticks.
   */
  default void tick() {
  }

}