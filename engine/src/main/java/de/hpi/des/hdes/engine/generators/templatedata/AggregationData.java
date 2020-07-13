package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import lombok.Getter;

@Getter
public class AggregationData {
    private final String pipelineId;
    private final int tupleLength;
    private final String aggregationValueType;
    private final String nativeAggregationValueType;
    private final int aggregationValueLength;
    private final int aggregationValueOffset;
    private final String aggregateImplementation;
    private final boolean shouldCountPerWindow;

    public AggregationData(final String pipelineId, final PrimitiveType[] types, 
                           final int aggregateValueIndex, final AggregateFunction aggregateFunction) {
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
        this.aggregationValueLength = types[aggregateValueIndex].getLength();

        switch(aggregateFunction) {
            case AVERAGE:
                aggregateImplementation = "(state * count + current) / (count + 1)"; 
                shouldCountPerWindow = true;
                aggregationValueType = types[aggregateValueIndex].getUppercaseName();
                nativeAggregationValueType = types[aggregateValueIndex].getLowercaseName();
                break;
            case COUNT:
                aggregateImplementation =  "state + 1";
                shouldCountPerWindow = false;
                aggregationValueType = "Int";
                nativeAggregationValueType = "int";
                break;
            case MINIMUM:
                aggregateImplementation = "Math.min(current, state)";
                shouldCountPerWindow = false;
                aggregationValueType = types[aggregateValueIndex].getUppercaseName();
                nativeAggregationValueType = types[aggregateValueIndex].getLowercaseName();
                break;
            case MAXIMUM:
                aggregateImplementation = "Math.max(current, state)";
                shouldCountPerWindow = false;
                aggregationValueType = types[aggregateValueIndex].getUppercaseName();
                nativeAggregationValueType = types[aggregateValueIndex].getLowercaseName();
                break;
            case SUM:
                aggregateImplementation = "current + state";
                shouldCountPerWindow = false;
                aggregationValueType = types[aggregateValueIndex].getUppercaseName();
                nativeAggregationValueType = types[aggregateValueIndex].getLowercaseName();
                break;
            default:
                aggregateImplementation = "state";
                shouldCountPerWindow = false;
                aggregationValueType = types[aggregateValueIndex].getUppercaseName();
                nativeAggregationValueType = types[aggregateValueIndex].getLowercaseName();
                break;
        }
    }
}