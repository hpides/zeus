package de.hpi.des.hdes.engine.graph.pipeline;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NetworkSourcePipeline extends Pipeline {

    @Getter
    private final NetworkSourceNode sourceNode;
    @Setter
    @Getter
    private Runnable pipelineObject;
    private boolean shutdownFlag;

    public NetworkSourcePipeline(NetworkSourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = (Runnable) pipelineKlass.getDeclaredConstructor(Buffer.class, childKlass).newInstance();// dispatcher.getReadByteBufferForPipeline(this),
                                                                                                                     // dispatcher,
                                                                                                                     // sourceNode.getHost(),
                                                                                                                     // sourceNode.getPort());
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void addParent(Pipeline pipeline, Node childNode) {
        this.setChild(pipeline);

    }

    @Override
    public void addOperator(Node operator, Node childNode) {
        // TODO Auto-generated method stub

    }

}