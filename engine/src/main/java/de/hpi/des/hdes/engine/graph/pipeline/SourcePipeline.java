package de.hpi.des.hdes.engine.graph.pipeline;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.Buffer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SourcePipeline extends Pipeline implements RunnablePipeline {

    private final BufferedSourceNode sourceNode;
    private Runnable pipelineObject;
    private boolean shutdownFlag;

    public SourcePipeline(BufferedSourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    void loadPipeline(Object child, Class childKlass) {
        Path javaFile = Paths.get(this.getFilePath());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        Path javaClass = javaFile.getParent().resolve(this.getPipelineId() + ".class");
        URL classUrl;
        try {
            classUrl = javaClass.getParent().toFile().toURI().toURL();
            URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { classUrl });
            pipelineKlass = Class.forName("de.hpi.des.hdes.engine.temp." + this.getPipelineId(), true, classLoader);
            Object temp = pipelineKlass.getDeclaredConstructor(Buffer.class, childKlass)
                    .newInstance(sourceNode.getSource().getInputBuffer(), child);
            pipelineObject = (Runnable) temp;
        } catch (MalformedURLException | ReflectiveOperationException | RuntimeException e) {
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
            log.error("Source had an exception: ", e);
            throw e;
        }
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

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