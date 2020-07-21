package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;

public class AJoinGenerator extends BinaryGeneratable {

    public AJoinGenerator(PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, int keyPositionLeft,
            int keyPositionRight, int windowLength) {
        super(leftTypes, rightTypes, keyPositionLeft, keyPositionRight, windowLength);
    }

    @Override
    public String generate(Pipeline pipeline) {
        // TODO
        return "";
    }

    @Override
    public String getOperatorId() {
        String hashBase = "ajoin";
        for (PrimitiveType t : leftTypes) {
            hashBase.concat(t.name());
        }
        hashBase.concat(Integer.toString(keyPositionLeft));
        for (PrimitiveType t : rightTypes) {
            hashBase.concat(t.name());
        }
        hashBase.concat(Integer.toString(keyPositionRight));
        return hashBase;
    }

}
