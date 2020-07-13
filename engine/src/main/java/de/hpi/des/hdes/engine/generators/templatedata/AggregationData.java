package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import lombok.Getter;

@Getter
public class AggregationData {
    private final String pipelineId;
    private final int tupleLength;
    private final PrimitiveType aggregationValueType;
    private final int aggregationValueOffset;
    private final String aggregateImplementation;
    private final boolean shouldCountPerWindow;
    private final InterfaceData[] interfaces;
    private final MaterializationData[] variables;
    private final String operators;

    public AggregationData(final String pipelineId, final PrimitiveType[] types, final int aggregateValueIndex,
            final AggregateFunction aggregateFunction, final InterfaceData[] interfaces,
            final MaterializationData[] variables, String operators) {
        this.pipelineId = pipelineId;

        int offset = 0;
        int size = 0;
        for (int i = 0; i < types.length; i++) {
            int length = types[i].getLength();
            size += length;
            if (i < aggregateValueIndex)
                offset += length;
        }
        this.aggregationValueOffset = offset;
        this.tupleLength = size;
        this.interfaces = interfaces;
        this.variables = variables;
        this.operators = operators;
        this.aggregateImplementation = aggregateFunction.getAggregateImplementation();
        this.shouldCountPerWindow = aggregateFunction.isShouldCountPerWindow();
        this.aggregationValueType = types[aggregateValueIndex];
    }
}