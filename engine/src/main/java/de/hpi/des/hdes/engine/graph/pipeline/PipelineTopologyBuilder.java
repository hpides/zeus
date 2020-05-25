package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import de.hpi.des.hdes.engine.graph.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.vulcano.SourceNode;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import de.hpi.des.hdes.engine.udf.Aggregator;
import lombok.Getter;

public class PipelineTopologyBuilder {
    @Getter
    private final List<Node> nodes = new LinkedList<>();

    public static PipelineTopology pipelineTopologyOf(Topology queryTopology) {
        List<Node> nodes = queryTopology.getTopologicalOrdering();
        List<Pipeline> pipelines = new ArrayList<>();

        List<Node> pipelineNodes;

        Map<Node, Pipeline> nodeToPipeline = new HashMap<Node, Pipeline>();
        Map<Node, Stack<List<Node>>> nodeToOperatorList = new HashMap<Node, Stack<List<Node>>>();

        for (Node node : Lists.reverse(nodes)) {
            // TODO: Add isPipelinebreaker to interfaecs
            if (node instanceof UnaryGenerationNode) {
                Pipeline currentPipeline;
                List<Node> operatorList;
                // TODO replace with compiled aggragator
                if (((UnaryGenerationNode) node).getOperator() instanceof Aggregator) {
                    // AggregationU
                    currentPipeline = new UnaryPipeline();
                    pipelines.add(currentPipeline);
                    if (!node.getChildren().isEmpty()) {
                        Pipeline childPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                        childPipeline.getParents().add(currentPipeline);
                    }
                    operatorList = ((UnaryPipeline) currentPipeline).getNodes();
                } else {
                    Node childNode = (Node) node.getChildren().toArray()[0];
                    operatorList = nodeToOperatorList.get(childNode).pop();
                    currentPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                }
                operatorList.add(node);
                nodeToPipeline.put(node, currentPipeline);
                Stack<List<Node>> listStack = new Stack<List<Node>>();
                listStack.add(operatorList);
                nodeToOperatorList.put(node, listStack);
            } else if (node instanceof BinaryGenerationNode) {
                // Binary Operator
                Pipeline currentPipeline = new BinaryPipeline();
                pipelines.add(currentPipeline);
                if (!node.getChildren().isEmpty()) {
                    Pipeline childPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                    childPipeline.getParents().add(currentPipeline);
                }
                List<Node> operatorListLeft = ((BinaryPipeline) currentPipeline).getLeftNodes();
                List<Node> operatorListRight = ((BinaryPipeline) currentPipeline).getRightNodes();
                operatorListLeft.add(node);
                operatorListRight.add(node);
                Stack<List<Node>> listStack = new Stack<List<Node>>();
                listStack.add(operatorListLeft);
                listStack.add(operatorListRight);
                nodeToOperatorList.put(node, listStack);
                nodeToPipeline.put(node, currentPipeline);
            } else {
                // throw new Exception("Unsupported Node type");
            }

        }

        return new PipelineTopology(pipelines);
    }

    public static PipelineTopologyBuilder newQuery() {
        return new PipelineTopologyBuilder();
    }

}
