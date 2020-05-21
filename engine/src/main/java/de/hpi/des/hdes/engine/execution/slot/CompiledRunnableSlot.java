package de.hpi.des.hdes.engine.execution.slot;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.execution.CompiledQuery;
import de.hpi.des.hdes.engine.execution.plan.Pipeline;
import de.hpi.des.hdes.engine.io.Buffer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompiledRunnableSlot implements Runnable {

    private Pipeline pipeline;
    private Runnable pipelineObject;
    private boolean shutdownFlag;
    private Buffer inputBuffer;

    public CompiledRunnableSlot(Pipeline pipeline, Buffer inputBuffer) {
        this.pipeline = pipeline;
        this.inputBuffer = inputBuffer;
        this.loadAndCompile();
    }

    public void loadAndCompile() {
        try {
            pipelineObject = (Runnable) this.pipeline.getClass().getDeclaredConstructor(Buffer.class)
                    .newInstance(this.inputBuffer);
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

    public void shutdown() {
        this.shutdownFlag = true;
    }

}