package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.graph.Topology;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * A query is a topology which can be uniquely defined.
 */
@Getter
public class Query {

  private final Topology topology;
  private final UUID id;

  public Query(final Topology topology) {
    this.topology = topology;
    this.id = UUID.randomUUID();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Query) {
      return this.getId() == ((Query) obj).getId();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id);
  }
}
