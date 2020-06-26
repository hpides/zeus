package de.hpi.des.hdes.engine.execution.buffer;

import java.nio.ByteBuffer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReadBuffer {

    private ByteBuffer buffer;
    private String pipelineID;
    private int pipelineBufferIndex;

}