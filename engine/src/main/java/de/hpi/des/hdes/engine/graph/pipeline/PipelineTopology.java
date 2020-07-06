package de.hpi.des.hdes.engine.graph.pipeline;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.TempSink;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class PipelineTopology {

    private final List<Pipeline> pipelines = new ArrayList<>();
    private final Map<Node, Pipeline> nodeToPipeline = new HashMap<Node, Pipeline>();
    private TempSink sink;
    private Dispatcher dispatcher;

    public static PipelineTopology pipelineTopologyOf(Topology queryTopology) {
        PipelineTopology pipelineTopology = new PipelineTopology();

        for (Node node : Lists.reverse(queryTopology.getTopologicalOrdering())) {
            ((GenerationNode) node).accept(pipelineTopology);
        }

        return pipelineTopology;
    }

    public void loadPipelines(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        try {
            this.sink = new TempSink(new FileWriter("output/compiled_out.csv"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        pipelines.get(0).loadPipeline(dispatcher, TempSink.class);
        for (Pipeline pipeline : pipelines.subList(1, pipelines.size())) {
            pipeline.loadPipeline(dispatcher, pipeline.getChild().getPipelineKlass());
        }
    }

    public List<Pipeline> getRunnablePiplines() {
        return this.pipelines;
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

    public Pipeline getPipelineByChild(Node node) {
        Node childNode = node.getChild();
        return this.nodeToPipeline.get(childNode);
    }

    public void addNodeToPipeline(Node node) {
        Node childNode = node.getChild();
        Pipeline currentPipeline = this.nodeToPipeline.get(childNode);
        currentPipeline.addOperator(node, childNode);
        this.nodeToPipeline.put(node, currentPipeline);
    }

    public void addPipelineAsParent(Pipeline pipeline, Node firstPipelineNode) {
        this.addPipelineAsLeaf(pipeline, firstPipelineNode);
        Node childNode = firstPipelineNode.getChild();
        this.nodeToPipeline.get(childNode).addParent(pipeline, childNode);
    }

    public void addPipelineAsLeaf(Pipeline pipeline, Node firstPipelineNode) {
        this.pipelines.add(pipeline);
        this.nodeToPipeline.put(firstPipelineNode, pipeline);
    }
}
