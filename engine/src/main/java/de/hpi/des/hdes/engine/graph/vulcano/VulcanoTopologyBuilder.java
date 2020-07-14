package de.hpi.des.hdes.engine.graph.vulcano;

import com.google.common.collect.Sets;

import org.jooq.lambda.tuple.Tuple2;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.astream.AStream;
import de.hpi.des.hdes.engine.cstream.CStream;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.predefined.ByteBufferIntSourceNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.NetworkSourceNode;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.Source;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

/**
 * The topology builder creates a DAG of nodes.
 */
public class VulcanoTopologyBuilder {

    @Getter
    private final List<Node> nodes = new LinkedList<>();

    /**
     * Adds a new parent-child relationship to the DAG.
     *
     * @param parent the parent node
     * @param child  the child node
     */
    public void addGraphNode(final Node parent, final Node child) {
        parent.addChild(child);
        this.nodes.add(child);
    }

    /**
     * Create a new query with this topology.
     *
     * @return a new builder
     */
    public static VulcanoTopologyBuilder newQuery() {
        return new VulcanoTopologyBuilder();
    }

    /**
     * The entry point for the definition of queries in HDES.
     *
     * @param source a source to read data from
     * @param <V>    type of the stream's elements
     * @return a new a stream
     */
    public <V> AStream<V> streamOf(final Source<V> source) {
        final SourceNode<V> sourceNode = new SourceNode<>(source);
        this.nodes.add(sourceNode);
        return new AStream<>(this, sourceNode);
    }

    /**
     * The entry point for the definition of queries in HDES.
     *
     * @param source a source to read data from
     * @return a new a stream
     */
    public CStream streamOfC(List<Tuple2<Integer, Boolean>> sourceList, final int outputTupleSize) {
        final ByteBufferIntSourceNode sourceNode = new ByteBufferIntSourceNode(sourceList, outputTupleSize);
        this.nodes.add(sourceNode);
        return new CStream(this, sourceNode);
    }

    /**
     * The entry point for the definition of queries in HDES.
     *
     * @param source a source to read data from
     * @return a new a stream
     */
    public CStream streamOfC(PrimitiveType[] types, String host, int port) {
        final NetworkSourceNode sourceNode = new NetworkSourceNode(types, host, port);
        this.nodes.add(sourceNode);
        return new CStream(this, sourceNode);
    }

    public Topology build() {
        return new Topology(Sets.newHashSet(this.nodes));
    }

    public Query buildAsQuery() {
        return new Query(new Topology(Sets.newHashSet(this.nodes)));
    }
}
