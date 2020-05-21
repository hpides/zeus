package de.hpi.des.hdes.engine.execution.plan;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@AllArgsConstructor
@Slf4j
public class Pipeline implements Runnable {
    private Class pipelinClass;

    Pipeline(final String queryFile, final String queryClass) {
        this.loadAndCompile(queryFile, queryClass);
    }

    private void loadAndCompile(final String queryFile, final String queryClass) {
        Path javaFile = Paths.get(this.pipeline.getQueryFile());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        Path javaClass = javaFile.getParent().resolve(this.pipeline.getQueryClass());
        URL classUrl;
        try {
            classUrl = javaClass.getParent().toFile().toURI().toURL();
            URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { classUrl });
            pipelineKlass = Class.forName(this.pipeline.getQueryClass(), true, classLoader);
        } catch (MalformedURLException | ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }
}