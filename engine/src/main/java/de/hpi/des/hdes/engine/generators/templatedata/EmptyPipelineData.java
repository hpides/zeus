package de.hpi.des.hdes.engine.generators.templatedata;

import java.util.Arrays;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.Getter;

@Getter
public class EmptyPipelineData {
    private final String className;
    private final String implementation;
    private final boolean hasChild;
    private final String nextPipeline;
    private final InterfaceData[] interfaces;
    private final MaterializationData[] variables;
    private final int outputEventLength;
    private final int inputEventLength;

    public EmptyPipelineData(final String className, final String implementation, final boolean hasChild,
            final Pipeline nextPipeline, final InterfaceData[] interfaces, final MaterializationData[] variables,
            PrimitiveType[] inputTypes, PrimitiveType[] outputTypes) {
        this.className = className;
        this.implementation = implementation;
        this.hasChild = hasChild;
        if (nextPipeline != null) {
            this.nextPipeline = nextPipeline.getPipelineId();
        } else {
            this.nextPipeline = null;
        }
        this.interfaces = interfaces;
        this.variables = variables;
        this.outputEventLength = Arrays.stream(outputTypes).mapToInt(t -> t.getLength()).sum() + 8 + 1;
        this.inputEventLength = Arrays.stream(inputTypes).mapToInt(t -> t.getLength()).sum() + 8 + 1;
    }
}