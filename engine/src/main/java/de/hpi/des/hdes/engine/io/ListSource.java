package de.hpi.des.hdes.engine.io;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.List;
import java.util.stream.Collectors;
import java.util.UUID;

public class ListSource<OUT> extends AbstractTopologyElement<OUT> implements Source<OUT> {

  private final List<AData<OUT>> list;
  private final String identifier;
  private int i = 0;

  public ListSource(final List<OUT> list) {
    this.list = list.stream().map(AData::of).collect(Collectors.toList());
    this.identifier = UUID.randomUUID().toString();
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
