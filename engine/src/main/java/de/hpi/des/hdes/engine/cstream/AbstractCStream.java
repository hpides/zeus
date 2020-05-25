package de.hpi.des.hdes.engine.cstream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

public abstract class AbstractCStream {
    protected VulcanoTopologyBuilder builder;
    protected Node node;

    protected AbstractCStream(final VulcanoTopologyBuilder builder, final Node node) {
        this.builder = builder;
        this.node = node;
    }

    public VulcanoTopologyBuilder getBuilder() {
        return this.builder;
    }

    protected Node getNode() {
        return this.node;
    }
}