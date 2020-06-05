package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * A node represents an element in the topology of logical plan in HDES.
 *
 * There are different types of nodes, representing different operations.
 */
@Slf4j
public abstract class Node {

  private final Collection<Node> children = new LinkedHashSet<>();
  private final Collection<Node> parents = new LinkedHashSet<>();
  private final List<Query> associatedQueries = new ArrayList<>();
  private final String nodeId;

  protected Node() {
    this.nodeId = UUID.randomUUID().toString();
  }

  protected Node(final String nodeId) {
    this.nodeId = nodeId;
  }

  public abstract void accept(NodeVisitor visitor);

  public void addAssociatedQuery(final Query query) {
    this.associatedQueries.add(query);
  }

  public void removeAssociatedQuery(final Query query) {
    this.associatedQueries.remove(query);
  }

  public void addChild(final Node node) {
    this.children.add(node);
    node.parents.add(this);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Node) {
      final Node node = (Node) obj;
      return node.nodeId.equals(this.nodeId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.nodeId.hashCode();
  }

  public Collection<Node> getChildren() {
    return this.children;
  }

  public Collection<Node> getParents() {
    return this.parents;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public List<Query> getAssociatedQueries() {
    return this.associatedQueries;
  }

  // TODO: This must be somehow dependent on a queryID -> unsure for now: how are
  // Pipelines connected to a query?
  public Node getChild() {
    return (Node) this.getChildren().toArray()[0];
  }
}
