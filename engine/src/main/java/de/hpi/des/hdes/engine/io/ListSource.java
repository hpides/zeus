package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.List;
import java.util.UUID;

public class ListSource<OUT> extends AbstractTopologyElement<OUT> implements Source<OUT> {

  private final String identifier;
  private final List<OUT> list;
  private int i = 0;

  public ListSource(final List<OUT> list) {
    this.identifier = UUID.randomUUID().toString();
    this.list = list;
  }

  @Override
  public String getIdentifier() {
    return this.identifier;
  }

  @Override
  public void read() {
    if (this.i < this.list.size()) {
      this.collector.collect(this.list.get(this.i++));
    }
  }

  public boolean isDone() {
    // This is not really thread-safe, as read() might potentially modify i while isDone() is
    // called, however, this is not a major problem and will be changed later when we decide on a
    // setup for tests
    return this.i == this.list.size();
  }
}
