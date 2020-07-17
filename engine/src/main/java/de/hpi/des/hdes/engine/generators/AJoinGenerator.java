package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AJoinGenerator implements Generatable {

    private final PrimitiveType[] leftTypes;
    private final PrimitiveType[] rightTypes;
    private final int keyPositionLeft;
    private final int keyPositionRight;
    private final int windowLength;

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
