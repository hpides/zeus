package de.hpi.des.hdes.engine.generators.templatedata;

import java.util.stream.Stream;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
public class JoinData {
    private final String pipelineId;
    private final int leftTupleLength;
    private final int rightTupleLength;
    private final PrimitiveType type;
    private final int windowLength;
    private final MaterializationData[] leftVariables;
    private final MaterializationData[] rightVariables;
    private final String leftOperators;
    private final String rightOperators;
    private final String leftKey;
    private final String rightKey;

    public JoinData(final String pipelineId, final PrimitiveType[] leftTypes, final PrimitiveType[] rightTypes,
            final int leftKeyIndex, final int rightKeyIndex, final int windowLength,
            final MaterializationData[] leftVariables, final MaterializationData[] rightVariables,
            final String leftOperators, final String rightOperators, final String leftKey, final String rightKey) {
        this.pipelineId = pipelineId;
        this.type = rightTypes[rightKeyIndex];
        this.leftTupleLength = Stream.of(leftTypes).mapToInt(t -> t.getLength()).sum();
        this.rightTupleLength = Stream.of(rightTypes).mapToInt(t -> t.getLength()).sum();
        this.windowLength = windowLength;
        this.leftVariables = leftVariables;
        this.rightVariables = rightVariables;
        this.leftOperators = leftOperators;
        this.rightOperators = rightOperators;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
    }
}