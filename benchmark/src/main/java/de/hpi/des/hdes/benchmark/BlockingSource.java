package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
public class BlockingSource<E> extends AbstractTopologyElement<E> implements Source<E> {

  private final SizedChunkedBuffer<E> queue;
  private final UUID id;

  public BlockingSource(int capacity) {
    this.queue = new SizedChunkedBuffer<>(capacity);
    this.id = UUID.randomUUID();
  }

  public BlockingSource() {
    this(Integer.MAX_VALUE);
  }

  public void offer(E event) {
    try {
      queue.add(AData.of(event));
    } catch (IllegalStateException e) {
      queue.drop();
      log.warn("Dropped input buffer of {}", event.getClass());
    }
  }

  @Override
  public String getIdentifier() {
    return this.id.toString();
  }

  @Override
  public void read() {
    AData<E> event = queue.poll();
    if (event != null) {
      collector.collect(event);
    }
  }
}