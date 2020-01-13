package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.Query;
import java.util.ArrayList;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class Slot implements Runnable {

    @Getter
    private final UUID topologyNodeId;
    @Getter
    @Setter
    private boolean alreadyRunning = false;
    @Getter
    private ArrayList<Query> associatedQueries = new ArrayList<>();
    private volatile boolean shutdownFlag = false;

    /**
     * runStep should never block indefinitely. As outgoing buffers might not have been flushed yet.
     */
    public abstract void runStep();

    public abstract void tick();

  public void shutdown() {
    log.info("Shutting Down Slot associated with Query: {}", associatedQueries.get(0).getId());
    this.shutdownFlag = true;
  }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted() && !shutdownFlag) {
            this.runStep();
            this.tick();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Slot) {
            return this.getTopologyNodeId() == ((Slot) obj).getTopologyNodeId();
        } else {
            return false;
        }
    }

    public void addAssociatedQuery(Query query) {
        this.associatedQueries.add(query);
    }

    public void removeAssociatedQuery(Query query) {
        this.associatedQueries.remove(query);
    }

}
