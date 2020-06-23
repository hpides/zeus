package de.hpi.des.hdes.engine.cstream;

import de.hpi.des.hdes.engine.generators.AJoinGenerator;
import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import de.hpi.des.hdes.engine.generators.FilterGenerator;
import de.hpi.des.hdes.engine.generators.FlatMapGenerator;
import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.generators.MapGenerator;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.JoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.AJoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSink;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSinkNode;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.vulcano.SinkNode;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

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
        final JoinGenerationNode child = new JoinGenerationNode(
                new JoinGenerator(keyExtractorLeft, keyExtractorRight, joinMapper));
        this.builder.addGraphNode(this.node, child);
        this.builder.addGraphNode(stream2.getNode(), child);
        return new CStream(this.builder, child);
    }

    /**
     * Ajoins the elements of two streams
     * 
     * @param rightStream     The (other) right stream
     * @param leftInputTypes  An array of types, in which order they appear on the
     *                        buffer for the current stream
     * @param rightInputTypes An array of types, in which order they appear on the
     *                        buffer for the right stream
     * @param leftKeyIndex    A 0-based index of which element in the values is the
     *                        key of the current stream
     * @param rightKeyIndex   A 0-based index of which element in the values is the
     *                        key of the right stream
     * @return Joined Stream
     */
    public CStream ajoin(final CStream rightStream, final PrimitiveType[] leftInputTypes,
            final PrimitiveType[] rightInputTypes, final int leftKeyIndex, final int rightKeyIndex) {
        final AJoinGenerationNode child = new AJoinGenerationNode(
                new AJoinGenerator(leftInputTypes, rightInputTypes, leftKeyIndex, rightKeyIndex));
        this.builder.addGraphNode(this.node, child);
        this.builder.addGraphNode(rightStream.getNode(), child);
        return new CStream(this.builder, child);
    }

    /**
     * Writes the elements of this stream into a sink.
     */
    public void to(BufferedSink sink) {
        final BufferedSinkNode child = new BufferedSinkNode(sink);
        this.builder.addGraphNode(this.node, child);
    }
}
