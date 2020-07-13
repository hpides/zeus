package de.hpi.des.hdes.engine.execution.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.slot.CompiledRunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.generators.LocalGenerator;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.Data;

// TODO engine: Maybe make this an interface only? The pipelineTopology stores the queries as well and could use a Helper Class (e.g. PipelineTopologyBuilder) to extend itself.
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
    private final PipelineTopology pipelineTopology;
    // oder
    // private final PipelineTopology pipelineTopology;

    public CompiledExecutionPlan(final Topology topology, final PipelineTopology pipelineTopology) {
        this.topology = topology;
        this.pipelineTopology = pipelineTopology;
    }

    private CompiledExecutionPlan() {
        this(Topology.emptyTopology(), new PipelineTopology());
    }

    /**
     * @return a list of runnable slots
     */
    public List<Pipeline> getRunnablePiplines() {
        return pipelineTopology.getRunnablePiplines();
    }

    public boolean isEmpty() {
        return this.topology.getNodes().isEmpty() && this.pipelineTopology.getPipelines().isEmpty();
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
            final Query query) {
        if (executionPlan.isEmpty()) {
            PipelineTopology newPipelineTopology = PipelineTopology.pipelineTopologyOf(query);

            LocalGenerator generator = new LocalGenerator(new PipelineTopology());
            generator.extend(newPipelineTopology);
            Dispatcher dispatcher = new Dispatcher(newPipelineTopology);

            newPipelineTopology.loadPipelines(dispatcher);

            return new CompiledExecutionPlan(query.getTopology(), newPipelineTopology);
        } else {
            // TODO engine
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
        return extend(emptyExecutionPlan(), query);
    }

    /**
     * Deletes a query from an execution plan
     *
     * @param executionPlan the execution plan to delete the query from
     * @param query         the query to delete
     * @return a new execution plan
     */
    public static CompiledExecutionPlan delete(final CompiledExecutionPlan executionPlan, final Query query) {
        // TODO engine
        return null;
    }

    public List<Pipeline> getRunnablePiplinesFor(Query query) {
        // TODO engine
        return null;
    }
}
