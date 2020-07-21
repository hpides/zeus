package de.hpi.des.hdes.engine.generators.templatedata;

import java.util.stream.Stream;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.AggregateFunction;
import lombok.Getter;
import de.hpi.des.hdes.engine.execution.Dispatcher;

@Getter
public class AggregationData {
    private final String pipelineId;
    private final int tupleLength;
    private final PrimitiveType aggregationValueType;
    private final String aggregateImplementation;
    private final boolean shouldCountPerWindow;
    private final InterfaceData[] interfaces;
    private final MaterializationData[] variables;
    private final String operators;
    private final String aggregationVariable;
    private final int vectorSize = Dispatcher.TUPLES_PER_VECTOR();
    private final int readVectorSize = Dispatcher.TUPLES_PER_READ_VECTOR();
    private final int outputEventLength;
    private final int inputEventLength;
    
    public AggregationData(final String pipelineId, final PrimitiveType[] types, final int aggregateValueIndex,
            final String aggregationVariable, final AggregateFunction aggregateFunction,
            final InterfaceData[] interfaces, final MaterializationData[] variables, String operators) {
        this.pipelineId = pipelineId;
        this.tupleLength = Stream.of(types).mapToInt(t -> t.getLength()).sum();
        this.interfaces = interfaces;
        this.variables = variables;
        this.operators = operators;
        this.aggregateImplementation = aggregateFunction.getAggregateImplementation();
        this.shouldCountPerWindow = aggregateFunction.isShouldCountPerWindow();
        this.aggregationValueType = types[aggregateValueIndex];
        this.aggregationVariable = aggregationVariable;

        this.outputEventLength = tupleLength + 8 + 1;
        this.inputEventLength = tupleLength + 8 + 1;
    }
}