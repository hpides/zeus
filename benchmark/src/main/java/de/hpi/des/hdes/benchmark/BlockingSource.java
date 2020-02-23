package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.execution.connector.SizedChunkedBuffer;
import de.hpi.des.hdes.engine.operation.AbstractSource;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
public class BlockingSource<E> extends AbstractSource<E> {

  private final SizedChunkedBuffer<E> queue;
  private final UUID id;

  public BlockingSource(int capacity, TimestampExtractor<E> timestampExtractor,
      WatermarkGenerator<E> watermarkGenerator) {
    super(timestampExtractor, watermarkGenerator);
    this.queue = new SizedChunkedBuffer<>(capacity);
    this.id = UUID.randomUUID();
  }

  public BlockingSource(int capacity) {
    this(capacity, TimestampExtractor.currentTimeNS(),
        WatermarkGenerator.milliseconds(100, 100));

  }


  public void offer(E event) {
    try {
      queue.add(event);
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
  public E readEvent() {
    return queue.poll();
  }
}