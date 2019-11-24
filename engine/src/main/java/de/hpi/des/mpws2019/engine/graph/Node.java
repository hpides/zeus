package de.hpi.des.mpws2019.engine.graph;

import java.util.Collection;
import java.util.LinkedHashSet;
import lombok.Getter;

@Getter
public abstract class Node {

  private final Collection<Node> children = new LinkedHashSet<>();
  private final Collection<Node> parents = new LinkedHashSet<>();

  public void addChild(final Node node) {
    this.children.add(node);
    node.parents.add(this);
  }

}
