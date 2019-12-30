package de.hpi.des.hdes.engine.graph;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class Node {

  private final Collection<Node> children = new LinkedHashSet<>();
  private final Collection<Node> parents = new LinkedHashSet<>();
  @Setter
  private UUID nodeId = UUID.randomUUID();

  public abstract void accept(NodeVisitor visitor);

  public void addChild(final Node node) {
    this.children.add(node);
    node.parents.add(this);
  }

  @Override
  public boolean equals(Object obj) {
      if(!this.getClass().equals(obj.getClass())) {
          log.warn("Error: Comparing Objects which are not alike");
      }
      Node nodeObj = (Node) obj;
      return this.getNodeId().equals(nodeObj.getNodeId());
  }

}
