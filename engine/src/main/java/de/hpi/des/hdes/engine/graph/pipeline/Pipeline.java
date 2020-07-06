package de.hpi.des.hdes.engine.graph.pipeline;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.DirectoryHelper;

import java.util.ArrayList;
import java.util.Arrays;
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
    @Setter
    protected Object pipelineObject;
    private static URLClassLoader tempClassLoader;

    public static URLClassLoader getClassLoader() {
        if (tempClassLoader == null) {
            try {
                tempClassLoader = URLClassLoader
                        .newInstance(new URL[] { new File("engine/src/main/java/").toURI().toURL() });
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        return tempClassLoader;
    }

    @Setter
    private Pipeline child;

    protected Pipeline() {
        this.pipelineId = "c".concat(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public abstract void accept(PipelineVisitor visitor);

    public abstract void addParent(Pipeline pipeline, Node childNode);

    public abstract void addOperator(Node operator, Node childNode);

    protected String getFilePath() {
        return DirectoryHelper.getTempDirectoryPath() + getPipelineId() + ".java";
    }

    protected void compileClass() {
        Path javaFile = Paths.get(this.getFilePath());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        List<String> optionList = new ArrayList<String>();
        if (this.getChild() != null) {
            optionList.addAll(Arrays.asList("-classpath", DirectoryHelper.getClassPathWithTempPackage()));
        }

        compiler.getTask(null, null, null, optionList, null, compiler.getStandardFileManager(null, null, null)
                .getJavaFileObjects(javaFile.toFile().getAbsolutePath())).call();
        try {
            pipelineKlass = Class.forName("de.hpi.des.hdes.engine.temp." + this.getPipelineId(), true,
                    Pipeline.getClassLoader());
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(childKlass)
                    .newInstance(dispatcher.getReadByteBufferForPipeline((UnaryPipeline) this), dispatcher);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }
}
