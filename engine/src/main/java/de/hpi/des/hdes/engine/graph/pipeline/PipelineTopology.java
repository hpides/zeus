package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class PipelineTopology {

    private final List<Pipeline> pipelines = new ArrayList<>();
    private final Map<GenerationNode, Pipeline> nodeToPipeline = new HashMap<GenerationNode, Pipeline>();
    private final Map<String, Pipeline> pipelineIdToPipeline = new HashMap<String, Pipeline>();

    public static PipelineTopology pipelineTopologyOf(Query query) {
        PipelineTopology pipelineTopology = new PipelineTopology();

        for (Node node : Lists.reverse(query.getTopology().getTopologicalOrdering())) {
            ((GenerationNode) node).accept(pipelineTopology);
        }
        pipelineTopology.getPipelines().stream().forEach(pipeline -> {
            pipeline.setInitialQueryId(query.getId().toString());
            pipelineTopology.pipelineIdToPipeline.put(pipeline.getPipelineId(), pipeline);
        });

        return pipelineTopology;
    }

    public List<Pipeline> extend(PipelineTopology newPipelineTopology) {
        List<Pipeline> newPipelines = new LinkedList<>();
        boolean foundMatch = false;
        for (Pipeline pipeline : newPipelineTopology.getPipelines()) {
            if (pipelineIdToPipeline.containsKey(pipeline.getPipelineId())) {
                Pipeline existingPipeline = this.pipelineIdToPipeline.get(pipeline.getPipelineId());
                existingPipeline.addQueryId(pipeline.getInitialQueryId());
                if (!foundMatch) {
                    pipeline.getChild().replaceParent(existingPipeline);
                    foundMatch = true;
                }
            } else {
                newPipelines.add(pipeline);
                pipelineIdToPipeline.put(pipeline.getPipelineId(), pipeline);
            }
        }
        pipelines.addAll(newPipelines);
        return newPipelines;
    }

    public static String getChildProcessMethod(Pipeline parent, Pipeline child) {
        if (child instanceof UnaryPipeline) {
            return "process";
        } else if (child instanceof JoinPipeline) {
            if (((JoinPipeline) child).getLeftParent().equals(parent)) {
                return "joinLeftPipeline";
            } else if (((JoinPipeline) child).getRightParent().equals(parent)) {
                return "joinRightPipeline";
            } else {
                log.error("Unkown parent pipeline in binary pipeline with id: {}", parent.getPipelineId());
                return "";
            }
        } else {
            return "process";
        }
    }

    public Pipeline getPipelineByChild(GenerationNode node) {
        GenerationNode childNode = (GenerationNode) node.getChild();
        return this.nodeToPipeline.get(childNode);
    }

    public void addNodeToPipeline(GenerationNode node) {
        GenerationNode childNode = (GenerationNode) node.getChild();
        Pipeline currentPipeline = this.nodeToPipeline.get(childNode);
        currentPipeline.addOperator((GenerationNode) node, (GenerationNode) childNode);
        this.nodeToPipeline.put(node, currentPipeline);
    }

    public void addPipelineAsParent(Pipeline pipeline, GenerationNode firstPipelineNode) {
        this.addPipelineAsLeaf(pipeline, firstPipelineNode);
        GenerationNode childNode = (GenerationNode) firstPipelineNode.getChild();
        this.nodeToPipeline.get(childNode).addParent(pipeline, childNode);
    }

    public void addPipelineAsLeaf(Pipeline pipeline, GenerationNode firstPipelineNode) {
        this.pipelines.add(pipeline);
        this.nodeToPipeline.put(firstPipelineNode, pipeline);
    }
}
