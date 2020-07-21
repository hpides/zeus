package de.hpi.des.hdes.engine.cstream;

import java.util.LinkedList;
import java.util.List;

import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;

import de.hpi.des.hdes.engine.generators.AJoinGenerator;
import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import de.hpi.des.hdes.engine.generators.FilterGenerator;
import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.generators.MapGenerator;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.predefined.ByteBufferIntListSinkNode;
import de.hpi.des.hdes.engine.graph.vulcano.SinkNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.JoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.AJoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.AggregationGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.FileSinkNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.operation.AggregateFunction;

public class CStream extends AbstractCStream {

    public CStream(final VulcanoTopologyBuilder builder, final GenerationNode node) {
        super(builder, node);
    }

    /**
     * Sums the elements of this stream.
     *
     * @return the aggregated stream
     */
    public CStream sum(final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        return this.aggregate(AggregateFunction.SUM, types, aggregateValueIndex, windowLength);
    }

    /**
     * Averages the elements of this stream.
     *
     * @return the aggregated stream
     */
    public CStream average(final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        return this.aggregate(AggregateFunction.AVERAGE, types, aggregateValueIndex, windowLength);
    }

    /**
     * Holds the minimum of all elements of this stream.
     *
     * @return the aggregated stream
     */
    public CStream minimum(final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        return this.aggregate(AggregateFunction.MINIMUM, types, aggregateValueIndex, windowLength);
    }

    /**
     * Holds the maximum of all elements of this stream.
     *
     * @return the aggregated stream
     */
    public CStream maximum(final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        return this.aggregate(AggregateFunction.MAXIMUM, types, aggregateValueIndex, windowLength);
    }

    /**
     * Counts the elements of this stream.
     *
     * @return the aggregated stream
     */
    public CStream count(final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        return this.aggregate(AggregateFunction.COUNT, types, aggregateValueIndex, windowLength);
    }

    /**
     * Maps the elements of this stream.
     *
     * @param mapper the mapper
     * @return the resulting stream with flat mapped elements
     */
    public CStream map(final Tuple mapper) {
        Tuple firstTuple = mapper.getFirst();
        final UnaryGenerationNode child = new UnaryGenerationNode(firstTuple.getTypes(), mapper.getTypes(),
                new MapGenerator(mapper));
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
    public CStream filter(final PrimitiveType[] types, final String filter) {
        final UnaryGenerationNode child = new UnaryGenerationNode(types, types, new FilterGenerator(types, filter));
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
    private CStream aggregate(AggregateFunction function, final PrimitiveType[] types, final int aggregateValueIndex, final int windowLength) {
        final AggregationGenerationNode child = new AggregationGenerationNode(types,
                new PrimitiveType[] { types[aggregateValueIndex] },
                new AggregateGenerator(function, aggregateValueIndex, windowLength));
        this.builder.addGraphNode(this.node, child);
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
    public CStream join(final CStream rightStream, final PrimitiveType[] leftInputTypes,
            final PrimitiveType[] rightInputTypes, final int leftKeyIndex, final int rightKeyIndex,
            final int windowLength) {
        final JoinGenerationNode child = new JoinGenerationNode(leftInputTypes, rightInputTypes,
                new JoinGenerator(leftInputTypes, rightInputTypes, leftKeyIndex, rightKeyIndex, windowLength));
        this.builder.addGraphNode(this.node, child);
        this.builder.addGraphNode(rightStream.getNode(), child);
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
            final PrimitiveType[] rightInputTypes, final int leftKeyIndex, final int rightKeyIndex,
            final int windowLength) {
        final AJoinGenerationNode child = new AJoinGenerationNode(leftInputTypes, rightInputTypes,
                new AJoinGenerator(leftInputTypes, rightInputTypes, leftKeyIndex, rightKeyIndex, windowLength));
        this.builder.addGraphNode(this.node, child);
        this.builder.addGraphNode(rightStream.getNode(), child);
        return new CStream(this.builder, child);
    }

    /**
     * Writes the elements of this stream into a list sink.
     */
    public void toStaticList(List<Tuple4<Long, Integer, Integer, Boolean>> resultList) {
        final ByteBufferIntListSinkNode child = new ByteBufferIntListSinkNode(resultList);
        this.builder.addGraphNode(this.node, child);
    }

    /**
     * Writes the elements of this stream into a file sink.
     */
    public void toFile(PrimitiveType[] types, int writeEveryX) {
        final FileSinkNode child = new FileSinkNode(types, writeEveryX);
        this.builder.addGraphNode(this.node, child);
    }

}
