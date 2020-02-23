package de.hpi.des.hdes.engine.graph;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Topology {

  private final Set<Node> nodes;

  public Topology(final Set<Node> nodes) {
    this.nodes = nodes;
  }

  public Topology() {
    this(new HashSet<>());
  }

  public static Topology emptyTopology() {
    return new Topology();
  }

  public static Topology of(final Set<Node> nodes) {
    return new Topology(nodes);
  }

  public Topology extend(final Topology other) {
    final Set<Node> allNodes = Sets.union(this.nodes, other.getNodes());
    return new Topology(allNodes);
  }

  public List<Node> getTopologicalOrdering() {
    final Map<Node, Long> nodeToIncEdges = new HashMap<>();
    final List<Node> result = new LinkedList<>();

    for (final Node node : this.nodes) {
      // only look at incoming edges that are part of this topology
      final long size = node.getParents().stream().filter(this.nodes::contains).count();
      nodeToIncEdges.put(node, size);
    }

    while (!nodeToIncEdges.keySet().isEmpty()) {
      final Node node = this.topSortNextNode(nodeToIncEdges);
      this.topSortReduceCount(nodeToIncEdges, node);
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
  private Node topSortNextNode(final Map<Node, Long> nodeToIncEdges) {
    for (final Node node : nodeToIncEdges.keySet()) {
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
   * @param node           node that was just added to the sort output
   */
  private void topSortReduceCount(final Map<Node, Long> nodeToIncEdges, final Node node) {
    final Collection<Node> outNodes = node.getChildren();
    for (final Node currNode : outNodes) {
      final long currIncEdges = nodeToIncEdges.get(currNode);
      nodeToIncEdges.put(currNode, currIncEdges - 1);
    }
  }
}
