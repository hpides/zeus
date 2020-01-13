package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.graph.Topology;
import java.util.UUID;
import lombok.Getter;

@Getter
public class Query {
    final private Topology topology;
    final private UUID id;

    public Query(Topology topology) {
        this.topology = topology;
        this.id = UUID.randomUUID();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Query) {
            return this.getId() == ((Query) obj).getId();
        } else {
            return false;
        }
    }
}
