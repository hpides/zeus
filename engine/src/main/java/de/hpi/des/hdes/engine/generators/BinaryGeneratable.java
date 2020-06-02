package de.hpi.des.hdes.engine.generators;

public interface BinaryGeneratable {

    String generate(final String execution, final String nextPipelineFunction, final boolean isLeft);

}
