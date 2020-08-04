package de.hpi.des.hdes.engine.generators.templatedata;

import java.util.stream.Stream;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.window.CWindow;
import lombok.Getter;

@Getter
public class AJoinData {
    private final String pipelineId;
    // TODO: Source distinguish between input and output tuples
    private final int leftTupleLength;
    private final int rightTupleLength;
    private final PrimitiveType type;
    private final long windowLength;
    private final long windowSlide;
    private final int vectorSize = Dispatcher.TUPLES_PER_VECTOR();
    private final int readVectorSize = Dispatcher.TUPLES_PER_READ_VECTOR();
    private final int eventCount = Dispatcher.TUPLES_PER_VECTOR()*Dispatcher.NUMBER_OF_VECTORS();
    private final InterfaceData[] interfaces;
    private final MaterializationData[] leftVariables;
    private final MaterializationData[] rightVariables;
    private final String leftOperators;
    private final String rightOperators;
    private final String leftKey;
    private final String rightKey;
    private final String writeOut;

    public AJoinData(final String pipelineId, final PrimitiveType[] leftTypes, final PrimitiveType[] rightTypes,
            final int leftKeyIndex, final int rightKeyIndex, final CWindow window, final InterfaceData[] interfaces,
            final MaterializationData[] leftVariables, final MaterializationData[] rightVariables,
            final String leftOperators, final String rightOperators, final String leftKey, final String rightKey,
            final String leftWriteout, final String rightWriteout) {
        this.pipelineId = pipelineId;
        this.type = rightTypes[rightKeyIndex];
        this.leftTupleLength = Stream.of(leftTypes).mapToInt(t -> t.getLength()).sum();
        this.rightTupleLength = Stream.of(rightTypes).mapToInt(t -> t.getLength()).sum();
        this.windowLength = window.getLength();
        this.windowSlide = window.getSlide();
        this.interfaces = interfaces;
        this.leftVariables = leftVariables;
        this.rightVariables = rightVariables;
        this.leftOperators = leftOperators;
        this.rightOperators = rightOperators;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.writeOut = leftWriteout.concat(rightWriteout);
    }
}
