package de.hpi.des.hdes.engine.generators.templatedata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class FileSinkData {

    final private String pipelineId;
    final private String inputTupleLength;
    final private String writeEveryX;
    final private String vectorSize;
    final private String readVectorSize;

}
