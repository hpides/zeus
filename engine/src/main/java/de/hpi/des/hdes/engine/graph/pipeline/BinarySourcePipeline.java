package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.io.Buffer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinarySourcePipeline extends BinaryPipeline implements RunnablePipeline {

    private boolean shutdownFlag;
    private Runnable pipelineObject;

    protected BinarySourcePipeline(BinaryPipeline pipeline) {
        super(pipeline.getLeftNodes(), pipeline.getRightNodes(), pipeline.getBinaryNode());
    }

    public static BinarySourcePipeline of(BinaryPipeline pipeline) {
        return new BinarySourcePipeline(pipeline);

    }

    @Override
    public void loadPipeline() {
        super.loadPipeline();
//        try {
//            pipelineObject = (Runnable) this.getPipelineKlass().getDeclaredConstructor(Buffer.class, Buffer.class)
//                    .newInstance(this.leftBuffer, this.rightBuffer);
//        } catch (ReflectiveOperationException | RuntimeException e) {
//            log.error("Slot had an exception during class load: ", e);
//        }
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

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

}