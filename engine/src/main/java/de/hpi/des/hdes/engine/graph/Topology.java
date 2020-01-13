package de.hpi.des.hdes.engine.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Topology {

    @Getter
    private final List<Node> nodes;

    public List<SourceNode> getSourceNodes() {
        LinkedList<SourceNode> sources = new LinkedList<>();
        for (Node node : this.nodes) {
            if (node instanceof SourceNode) {
                sources.add((SourceNode) node);
            }
        }
        return sources;
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void removeNodes(List<Node> nodeList) {
        nodes.removeAll(nodeList);
    }

    public Node getNodeById(UUID nodeId) {
        return this.nodes.stream()
                .filter(node -> node.getNodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Couldn't find node with given ID"));
    }

    public List<Node> getTopologicalOrdering() {
        final Map<Node, Integer> nodeToIncEdges = new HashMap<>();
        final List<Node> result = new LinkedList<>();

        for (Node node : this.nodes) {
            nodeToIncEdges.put(node, node.getParents().size());
        }

        while (nodeToIncEdges.keySet().size() != 0) {
            Node node = topSortNextNode(nodeToIncEdges);
            topSortReduceCount(nodeToIncEdges, node);
            result.add(node);
        }

        return result;
    }

    /**
     * Retrieves the next node without incoming edges.
     *
     * @param nodeToIncEdges nodes mapped to the count of incoming edges
     * @return a random node with incoming edges
     */
    private Node topSortNextNode(Map<Node, Integer> nodeToIncEdges) {
        for (Node node : nodeToIncEdges.keySet()) {
            if (nodeToIncEdges.get(node) == 0) {
                nodeToIncEdges.remove(node);
                return node;
            }
        }
        throw new IllegalStateException("The graph has no topological ordering. Use a DAG.");
    }

    /**
     * Decreases the amount of inc. edges for all the children of the node.
     *
     * @param nodeToIncEdges A map of the remaining unsorted nodes and their remaining inc edge count.
     * @param node node that was just added to the sort output
     */
    private void topSortReduceCount(Map<Node, Integer> nodeToIncEdges, Node node) {
        Collection<Node> outNodes = node.getChildren();
        for (Node currNode : outNodes) {
            int currIncEdges = nodeToIncEdges.get(currNode);
            nodeToIncEdges.put(currNode, currIncEdges - 1);
        }
    }
}
