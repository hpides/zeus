package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.window.CWindow;

public class JoinGenerator extends BinaryGeneratable {

    public JoinGenerator(PrimitiveType[] leftTypes, PrimitiveType[] rightTypes, int keyPositionLeft,
            int keyPositionRight, final CWindow window) {
        super(leftTypes, rightTypes, keyPositionLeft, keyPositionRight, window);
    }

    @Override
    public String generate(Pipeline pipeline) {
        // TODO
        return "";
    }

    @Override
    public String getOperatorId() {
        String hashBase = "join";
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
