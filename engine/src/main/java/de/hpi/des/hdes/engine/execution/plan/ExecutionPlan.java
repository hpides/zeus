package de.hpi.des.hdes.engine.execution.plan;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.graph.Node;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutionPlan {

    private final List<Slot> slots;

    private ExecutionPlan(final List<Slot> slots) {
        this.slots = slots;
    }

    public List<Slot> getSlots() {
        return this.slots;
    }

    public void addSlot(Slot slot) {
        this.slots.add(slot);
    }

    public void removeSlot(Slot slot) {
        this.slots.remove(slot);
    }

    public Slot getSlotById(UUID slotId) {
        return this.slots.stream()
                .filter(slot -> slot.getTopologyNodeId().equals(slotId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Couldn't find slot with given ID"));
    }

    public static ExecutionPlan from(final Query query) {
        return extend(query, new HashMap<>());
    }

    public static ExecutionPlan extend(final Query query,
            final Map<UUID, SourceSlot<?>> matchingUUIDtoSourceSlotMap) {
        final List<Node> sortedNodes = query.getTopology().getTopologicalOrdering();
        final PushExecutionPlanBuilder visitor = new PushExecutionPlanBuilder(matchingUUIDtoSourceSlotMap, query);

        for (Node node : sortedNodes) {
            node.accept(visitor);
        }

        final List<Slot> slots = visitor.getSlots();
        return new ExecutionPlan(slots);
    }
}
