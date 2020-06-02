package de.hpi.des.hdes.engine.graph.pipeline;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class Pipeline {

    protected Class pipelineKlass;
    private final String pipelineId;
    private Object pipelineObject;
    private final List<Pipeline> parents = new ArrayList<>();

    @Setter
    private Pipeline child;

    protected Pipeline() {
        this.pipelineId = "c".concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    void addParent(Pipeline pipeline) {
        this.parents.add(pipeline);
        pipeline.setChild(this);
    }

    public abstract void accept(PipelineVisitor visitor);

    protected String getFilePath() {
        return "/Users/nils/Documents/MP/HDES/engine/src/main/java/de/hpi/des/hdes/engine/temp/" + getPipelineId()
                + ".java";
    }

    // TODO code-generation: call after file was generated
    void loadPipeline(Object child, Class childKlass) {
        Path javaFile = Paths.get(this.getFilePath());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        Path javaClass = javaFile.getParent().resolve(this.pipelineId+".class");
        // Path javaClass = javaFile.getParent().resolve("HardCodedAggregation.class");
        URL classUrl;
        try {
            classUrl = javaClass.getParent().toFile().toURI().toURL();
            URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { classUrl });
            pipelineKlass = Class.forName("de.hpi.des.hdes.engine.temp."+this.pipelineId, true, classLoader);
            pipelineObject = pipelineKlass.getDeclaredConstructor(childKlass).newInstance(child);
        } catch (MalformedURLException | ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    public void addLeftParent(Pipeline leftPipeline) {
        this.parents.add(leftPipeline);
        leftPipeline.setChild(this);
    }

    public void addRightParent(Pipeline rightPipeline) {
        this.parents.add(rightPipeline);
        rightPipeline.setChild(this);
    }
}
