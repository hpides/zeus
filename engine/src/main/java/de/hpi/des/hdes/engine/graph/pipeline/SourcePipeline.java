package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.io.Buffer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SourcePipeline extends Pipeline implements RunnablePipeline {

    private final Buffer buffer;
    private boolean shutdownFlag;
    private Runnable pipelineObject;

    public SourcePipeline(List<Node> nodes, Buffer buffer) {
        super(nodes);
        this.buffer = buffer;
    }

    public void shutdown() {

    }

    @Override
    public void loadPipeline() {
        super.loadPipeline();
        try {
            pipelineObject = (Runnable) this.getPipelineKlass().getDeclaredConstructor(Buffer.class)
                    .newInstance(this.buffer);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted() && !this.shutdownFlag) {
                pipelineObject.run();
            }
            log.debug("Stopped running {}", this);
        } catch (final RuntimeException e) {
            log.error("Slot had an exception: ", e);
            throw e;
        }
    }
}