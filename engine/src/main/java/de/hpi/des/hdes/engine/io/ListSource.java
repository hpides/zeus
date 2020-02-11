package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.operation.AbstractSource;
import de.hpi.des.hdes.engine.udf.TimestampExtractor;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;

import java.util.List;
import java.util.UUID;

public class ListSource<OUT> extends AbstractSource<OUT> {

  private final List<OUT> list;
  private final String identifier;
  private int i = 0;

  public ListSource(final List<OUT> list) {
    super(e -> 0L, new WatermarkGenerator<>(0, 1000));
    this.list = list;
    this.identifier = UUID.randomUUID().toString();
  }

  public ListSource(final List<OUT> list, WatermarkGenerator<OUT> watermarkGenerator, TimestampExtractor<OUT> timestampExtractor) {
    super(timestampExtractor, watermarkGenerator);
    this.list = list;
    this.identifier = UUID.randomUUID().toString();
  }

  @Override
  public String getIdentifier() {
    return this.identifier;
  }

  @Override
  public OUT readEvent() {
    if (this.i < this.list.size()) {
      return this.list.get(this.i++);
    } else {
      return null;
    }
  }

  public boolean isDone() {
    // This is not really thread-safe, as read() might potentially modify i while isDone() is
    // called, however, this is not a major problem and will be changed later when we decide on a
    // setup for tests
    return this.i == this.list.size();
  }
}
