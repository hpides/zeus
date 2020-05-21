package de.hpi.des.hdes.engine.execution.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.slot.CompiledRunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * The execution plan describes the current way the engine operates.
 *
 * The plan is mainly based on two parts: a list of slots {@link Slot} and a
 * mapping. The list of slots is used to execute all slots accordingly. The
 * mapping allows for interaction between the {@link Topology} and the execution
 * plan. For example, it allows the deletion of a node.
 */
@Data
public class CompiledExecutionPlan {
    private final Topology topology;
    private final List<CompiledRunnableSlot> slots;

    private final List<Pipeline> pipelines;
    // oder
    // private final PipelineTopology pipelineTopology;

    public CompiledExecutionPlan(final Topology topology, final List<Pipeline> pipelines) {
        this.topology = topology;
        this.pipelines = pipelines;
        this.slots = slots;
    }

    private CompiledExecutionPlan() {
        this(Topology.emptyTopology(), Lists.newArrayList(), Lists.newArrayList());
    }

    /**
     * @return a list of runnable slots
     */
    public List<CompiledRunnableSlot> getRunnableSlots() {
        return slots;
    }

    public boolean isEmpty() {
        return this.topology.getNodes().isEmpty() && this.slots.isEmpty();
    }

    public static CompiledExecutionPlan emptyExecutionPlan() {
        return new CompiledExecutionPlan();
    }

    /**
     * Extends an execution plan with a new query.
     *
     * @param executionPlan the plan to extend
     * @param topology      the new query
     * @return a new plan
     */
    public static CompiledExecutionPlan extend(final CompiledExecutionPlan executionPlan,
            final Topology queryTopology) {
        if (executionPlan.isEmpty()) {
            List<Pipeline> pipelineTopology = CodeGenerator.generate(queryTopology);
            // Call code generator for topology and get Pipelines back
            // Translate Pipelines into Slots
            // return new object
            return new CompiledExecutionPlan(queryTopology, pipelineTopology);
        }

        return null;
    }

    /**
     * Creates a execution plan with a single query.
     *
     * @param query the query to use
     * @return a new execution plan
     */
    public static CompiledExecutionPlan createPlan(final Query query) {
        return extend(emptyExecutionPlan(), query.getTopology());
    }

    /**
     * Deletes a query from an execution plan
     *
     * @param executionPlan the execution plan to delete the query from
     * @param query         the query to delete
     * @return a new execution plan
     */
    public static CompiledExecutionPlan delete(final CompiledExecutionPlan executionPlan, final Query query) {

        return null;
    }
}
