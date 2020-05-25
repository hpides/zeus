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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class Pipeline {
    private Class pipelineKlass;

    private final List<Node> nodes;
    private final String pipelineId;
    private final List<Pipeline> parents = new ArrayList<>();
    @Setter
    private Pipeline child;

    protected Pipeline(final List<Node> nodes) {
        this.nodes = nodes;
        this.pipelineId = "c".concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public static Pipeline of(final List<Node> nodes) {
        return new Pipeline(nodes);
    }

    void addParent(Pipeline pipeline) {
        this.parents.add(pipeline);
        pipeline.setChild(this);
    }

    private String getFilePath() {
        // TODO code-generation: Define path to files in a central place
        return "";
    }

    // TODO code-generation: call after file was generated
    void loadPipeline() {
        Path javaFile = Paths.get(this.getFilePath());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        Path javaClass = javaFile.getParent().resolve(this.pipelineId);
        URL classUrl;
        try {
            classUrl = javaClass.getParent().toFile().toURI().toURL();
            URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { classUrl });
            pipelineKlass = Class.forName(this.pipelineId, true, classLoader);
        } catch (MalformedURLException | ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }
}
