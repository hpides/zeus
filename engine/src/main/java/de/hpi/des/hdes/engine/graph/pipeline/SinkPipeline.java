package de.hpi.des.hdes.engine.graph.pipeline;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler;
import java.nio.file.Path;
import java.nio.file.Paths;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.graph.pipeline.node.BufferedSinkNode;

@Slf4j
public class SinkPipeline extends Pipeline {

  private final BufferedSinkNode sinkNode;
  private Runnable pipelineObject;
  private boolean shutdownFlag;
  @Getter
  private Pipeline parent;

  public SinkPipeline(BufferedSinkNode sinkNode) {
    super(sinkNode.getInputTypes());
    this.sinkNode = sinkNode;
  }

  @Override
  public void accept(PipelineVisitor visitor) {
    visitor.visit(this);
  }

  void loadPipeline(Object child) {
    Path javaFile = Paths.get(this.getFilePath());
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
    Path javaClass = javaFile.getParent().resolve(this.getPipelineId() + ".class");
    try {
      URL classURl = javaClass.getParent().toFile().toURI().toURL();
      URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] { classURl });
      pipelineKlass = Class.forName("de.hpi.des.hdes.engine.temp." + this.getPipelineId(), true, classLoader);
      Object temp = pipelineKlass.getDeclaredConstructor(Buffer.class)
          .newInstance(sinkNode.getSink().getOutputBuffer());
      pipelineObject = (Runnable) temp;
    } catch (MalformedURLException | ReflectiveOperationException | RuntimeException e) {
      log.error("Slot had an exception during class load: ", e);
    }
  }

  @Override
  public void addParent(Pipeline pipeline, GenerationNode childNode) {
    parent = pipeline;
    pipeline.setChild(this);
  }

  @Override
  public void addOperator(GenerationNode operator, GenerationNode childNode) {
    // TODO Auto-generated method stub

  }
}
