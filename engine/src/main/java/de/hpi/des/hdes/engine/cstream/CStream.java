package de.hpi.des.hdes.engine.cstream;

import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import de.hpi.des.hdes.engine.generators.FilterGenerator;
import de.hpi.des.hdes.engine.generators.FlatMapGenerator;
import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.generators.MapGenerator;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.vulcano.SinkNode;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.operation.Sink;

public class CStream extends AbstractCStream {

    public CStream(final VulcanoTopologyBuilder builder, final Node node) {
        super(builder, node);
    }

    /**
     * Flat maps the elements of this stream.
     *
     * @param mapper the mapper
     * @return the resulting stream with flat mapped elements
     */
    public CStream flatMap(final String mapper) {
        final UnaryGenerationNode child = new UnaryGenerationNode(new FlatMapGenerator(mapper));
        this.builder.addGraphNode(this.node, child);
        return new CStream(this.builder, child);
    }

    /**
     * Maps the elements of this stream.
     *
     * @param mapper the mapper
     * @return the resulting stream with flat mapped elements
     */
    public CStream map(final String mapper) {
        final UnaryGenerationNode child = new UnaryGenerationNode(new MapGenerator(mapper));
        this.builder.addGraphNode(this.node, child);
        return new CStream(this.builder, child);
    }

    /**
     * Filters the elements of this stream.
     *
     * Only elements fulfilling the filter predicate remain in the stream.
     *
     * @param filter the predicate
     * @return the filtered stream
     */
    public CStream filter(final String filter) {
        final UnaryGenerationNode child = new UnaryGenerationNode(new FilterGenerator(filter));
        this.builder.addGraphNode(this.node, child);
        return new CStream(this.builder, child);
    }

    /**
     * Filters the elements of this stream.
     *
     * Only elements fulfilling the filter predicate remain in the stream.
     *
     * @param filter the predicate
     * @return the filtered stream
     */
    public CStream aggregate() {
        final UnaryGenerationNode child = new UnaryGenerationNode(new AggregateGenerator());
        this.builder.addGraphNode(this.node, child);
        return new CStream(this.builder, child);
    }

    /**
     * Joins the elements of two stream.
     *
     *
     * @param stream2 second cstream
     * @return joined cstream
     */
    public CStream join(final CStream stream2, final String keyExtractorLeft, final String keyExtractorRight,
            final String joinMapper) {
        final BinaryGenerationNode child = new BinaryGenerationNode(
                new JoinGenerator(keyExtractorLeft, keyExtractorRight, joinMapper));
        this.builder.addGraphNode(this.node, child);
        this.builder.addGraphNode(stream2.getNode(), child);
        return new CStream(this.builder, child);
    }

    /**
     * Writes the elements of this stream into a sink.
     *
     * @return the final builder
     */
    public VulcanoTopologyBuilder to() {
        return this.builder;
    }
}