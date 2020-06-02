package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.SourceNode;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import de.hpi.des.hdes.engine.udf.Aggregator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PipelineTopologyBuilder {
    @Getter
    private final List<Node> nodes = new LinkedList<>();

    public static PipelineTopology pipelineTopologyOf(Topology queryTopology) {
        List<Node> nodes = queryTopology.getTopologicalOrdering();
        List<Pipeline> pipelines = new ArrayList<>();

        List<Node> pipelineNodes;

        Map<Node, Pipeline> nodeToPipeline = new HashMap<Node, Pipeline>();
        Map<Node, Stack<List<Node>>> nodeToInternalPipelineList = new HashMap<Node, Stack<List<Node>>>();

        for (Node node : Lists.reverse(nodes)) {
            // TODO: Add isPipelinebreaker to interfaces
            if (node instanceof UnaryGenerationNode) {
                log.info("Unary: {}", node.getClass());
                Pipeline currentPipeline;
                List<Node> internalOperatorList;
                // TODO replace with compiled aggregator
                if (((UnaryGenerationNode) node).getOperator() instanceof AggregateGenerator) {
                    // Aggregation
                    currentPipeline = new UnaryPipeline();
                    pipelines.add(currentPipeline);
                    if (!node.getChildren().isEmpty()) {
                        Pipeline childPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                        Node childNode = (Node) node.getChildren().toArray()[0];
                        addParent(node, childPipeline, currentPipeline, childNode);
                    }
                    internalOperatorList = ((UnaryPipeline) currentPipeline).getNodes();
                } else if (!node.getChildren().isEmpty()) {
                    Node childNode = (Node) node.getChildren().toArray()[0];
                    internalOperatorList = nodeToInternalPipelineList.get(childNode).pop();
                    currentPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                } else {
                    currentPipeline = new UnaryPipeline();
                    pipelines.add(currentPipeline);
                    internalOperatorList = ((UnaryPipeline) currentPipeline).getNodes();
                }
                internalOperatorList.add(node);
                nodeToPipeline.put(node, currentPipeline);
                Stack<List<Node>> listStack = new Stack<List<Node>>();
                listStack.add(internalOperatorList);
                nodeToInternalPipelineList.put(node, listStack);
            } else if (node instanceof BinaryGenerationNode) {
                log.info("Binary {}", node.getClass());
                // Binary Operator
                Pipeline currentPipeline = new BinaryPipeline((BinaryGenerationNode) node);
                pipelines.add(currentPipeline);
                if (!node.getChildren().isEmpty()) {
                    Pipeline childPipeline = nodeToPipeline.get(node.getChildren().toArray()[0]);
                    Node childNode = (Node) node.getChildren().toArray()[0];
                    addParent(node, childPipeline, currentPipeline, childNode);
                }
                List<Node> operatorListLeft = ((BinaryPipeline) currentPipeline).getLeftNodes();
                List<Node> operatorListRight = ((BinaryPipeline) currentPipeline).getRightNodes();
                Stack<List<Node>> listStack = new Stack<List<Node>>();
                listStack.add(operatorListLeft);
                listStack.add(operatorListRight);
                nodeToInternalPipelineList.put(node, listStack);
                nodeToPipeline.put(node, currentPipeline);
            } else if (node instanceof BufferedSourceNode) {
                Object childNode = node.getChildren().toArray()[0];
                Pipeline childPipeline = nodeToPipeline.get(childNode);
                SourcePipeline sourcePipeline = new SourcePipeline((BufferedSourceNode) node);
                addParent(node, childPipeline, sourcePipeline, (Node) childNode);
                pipelines.add(sourcePipeline);
            } else {
                log.info("Unknown {}", node.getClass());
            }

        }

        return new PipelineTopology(pipelines);
    }

    private static void addParent(Node node, Pipeline childPipeline, Pipeline sourcePipeline, Node childNode) {
        if (childPipeline instanceof BinaryPipeline) {
            // TODO make it more efficient
            // TODO if an aggregation comes before a join, the nodes are empty
            if (((BinaryPipeline) childPipeline).getLeftNodes().contains(childNode)) {
                childPipeline.addLeftParent(sourcePipeline);
            } else if (((BinaryPipeline) childPipeline).getRightNodes().contains(childNode)) {
                childPipeline.addRightParent(sourcePipeline);
            } else {
                log.error("Something unpredictable in a class you do not want to touch happened (RUN!!!): {}",
                        node.getClass());
            }

        } else if (childPipeline instanceof UnaryPipeline) {
            childPipeline.addParent(sourcePipeline);
        }
    }

    public static PipelineTopologyBuilder newQuery() {
        return new PipelineTopologyBuilder();
    }

}
