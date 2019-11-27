package de.hpi.des.mpws2019.engine.graph;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;
import lombok.Getter;

@Getter
public abstract class Node {

  private final Collection<Node> children = new LinkedHashSet<>();
  private final Collection<Node> parents = new LinkedHashSet<>();
  private final UUID nodeId = UUID.randomUUID();

  public abstract void accept(NodeVisitor visitor);

  public void addChild(final Node node) {
    this.children.add(node);
    node.parents.add(this);
  }

}
