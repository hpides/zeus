package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;

public class BufferedSourceNode extends Node {

    @Getter
    private final BufferedSource source;

    public BufferedSourceNode(BufferedSource source) {
        this.source = source;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

}
