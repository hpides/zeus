package de.hpi.des.hdes.engine.generators.templatedata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class NetworkSourceData {
    private String pipelineId;
    private String eventLength;
    private String host;
    private String port;
}