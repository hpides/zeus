package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.StringAssert;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.JoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.BufferedSinkNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.BufferedSourceNode;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

public class CodeAssert extends StringAssert {

  private PipelineTopology pipelineTopology;
  private VulcanoTopologyBuilder builder;
  private HashMap<Pipeline, String> code;
  private Matcher matcher;
  private Pipeline matched;

  protected CodeAssert(String code, PipelineTopology pt, VulcanoTopologyBuilder b) {
    super(code);
    pipelineTopology = pt;
    builder = b;
  }

  public static CodeAssert assertThat(PipelineTopology pipelineTopology, VulcanoTopologyBuilder builder) {
    return new CodeAssert("code", pipelineTopology, builder);
  }

  private String errorMessage(String type, String name, int actual, int expected) {
    String pipelineTemplate = "Topology consists of %s %s %s instead of %s";
    return String.format(pipelineTemplate, actual, name, type, expected);
  }

  private String pipelineErrorMessage(String name, int actual, int expected) {
    return errorMessage(actual == 1 ? "pipeline" : "pipelines", name, actual, expected);
  }

  private String nodeErrorMessage(String name, int actual, int expected) {
    return errorMessage(actual == 1 ? "node" : "nodes", name, actual, expected);
  }

  private String patternErrorMessage(String pattern, String name) {
    return "Could not find ".concat(pattern).concat(" in ").concat(name)
        .concat(code.get(matched).substring(matcher.start()));
  }

  public CodeAssert hasSourcePipelines(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Pipeline p : pipelineTopology.getPipelines()) {
      if (p instanceof BufferedSourcePipeline) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(pipelineErrorMessage("Source", actual, amount))
        .isEqualTo(amount);
    return this;
  }

  public CodeAssert hasUnaryPipelines(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Pipeline p : pipelineTopology.getPipelines()) {
      if (p instanceof UnaryPipeline) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(pipelineErrorMessage("Unary", actual, amount))
        .isEqualTo(amount);
    return this;
  }

  public CodeAssert hasBinaryPipelines(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Pipeline p : pipelineTopology.getPipelines()) {
      if (p instanceof JoinPipeline) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(pipelineErrorMessage("Binary", actual, amount))
        .isEqualTo(amount);
    return this;
  }

  public CodeAssert hasSinkPipelines(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Pipeline p : pipelineTopology.getPipelines()) {
      if (p instanceof SinkPipeline) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(pipelineErrorMessage("Unary", actual, amount))
        .isEqualTo(amount);
    return this;
  }

  public CodeAssert hasSourceNodes(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Node n : builder.getNodes()) {
      if (n instanceof BufferedSourceNode) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(nodeErrorMessage("Source", actual, amount)).isEqualTo(amount);
    return this;
  }

  public CodeAssert hasUnaryNodes(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Node n : builder.getNodes()) {
      if (n instanceof UnaryGenerationNode) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(nodeErrorMessage("Unary", actual, amount)).isEqualTo(amount);
    return this;
  }

  public CodeAssert hasBinaryNodes(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Node n : builder.getNodes()) {
      if (n instanceof JoinGenerationNode) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(nodeErrorMessage("Binary", actual, amount)).isEqualTo(amount);
    return this;
  }

  public CodeAssert hasSinkNodes(int amount) {
    // Maybe extract into private method
    int actual = 0;
    for (Node n : builder.getNodes()) {
      if (n instanceof BufferedSinkNode) {
        actual++;
      }
    }
    Assertions.assertThat(actual).overridingErrorMessage(nodeErrorMessage("Sink", actual, amount)).isEqualTo(amount);
    return this;
  }

  public CodeAssert gotGenerated() {
    code = new HashMap<>();
    for (Pipeline p : pipelineTopology.getPipelines()) {
      Path path = Paths.get("src/main/java/de/hpi/des/hdes/engine/temp/".concat(p.getPipelineId()).concat(".java"));
      Assertions.assertThat(path.toFile()).overridingErrorMessage(
          String.format("Could not find file %s for pipeline %s", path.toString(), p.getPipelineId())).exists();
      try {
        code.put(p, Files.readString(path));
      } catch (IOException e) {
      }
    }
    return this;
  }

  public CodeAssert isConnected() {
    if (code == null) {
      this.gotGenerated();
    }
    for (Pipeline p : pipelineTopology.getPipelines()) {
      if (p.getChild() != null) {
        Assertions.assertThat(code.get(p)).containsPattern(
            Pattern.compile("private final ".concat(p.getChild().getPipelineId()).concat(" nextPipeline;")));
      }
    }
    return this;
  }

  private Pattern compileToPattern(String pattern) {
    return Pattern.compile(pattern.replaceAll(".", "\\."));
  }

  private Pattern nestedPattern(String pattern) {
    return compileToPattern("\\s*)*\\s*{\\s*".concat(pattern));
  }

  private Pattern deepPattern(String pattern) {
    return compileToPattern(".*".concat(pattern));
  }

  private Pattern varPattern(String name, String value) {
    return compileToPattern(name.concat("\\s*=\\s*").concat(value).concat("\\s*;"));
  }

  public CodeAssert traverseAST(String entryPattern) {
    for (Pipeline p : pipelineTopology.getPipelines()) {
      matcher = compileToPattern(entryPattern).matcher(code.get(p));
      if (matcher.find()) {
        matched = p;
        break;
      }
    }
    Assertions.assertThat(matched != null)
        .overridingErrorMessage(String.format("Could not find entryPattern ".concat(entryPattern))).isTrue();
    return this;
  }

  public CodeAssert followedBy(String pattern) {
    if (matched == null)
      throw new AssertionError("Did not start AST traversal.");
    String e = patternErrorMessage(pattern, "followed");
    Assertions.assertThat(matcher.usePattern(nestedPattern(pattern)).find()).overridingErrorMessage(e).isTrue();
    return this;
  }

  public CodeAssert hasNested(String pattern) {
    if (matched == null)
      throw new AssertionError("Did not start AST traversal.");
    String e = patternErrorMessage(pattern, "nested");
    Assertions.assertThat(matcher.usePattern(deepPattern(pattern)).find()).overridingErrorMessage(e).isTrue();
    return this;
  }

  public CodeAssert endsPipeline() {
    if (matched == null)
      throw new AssertionError("Did not start AST traversal.");
    String e = patternErrorMessage("nextPipeline.process", "ends");
    Assertions.assertThat(matcher.usePattern(nestedPattern("(this.)??nextPipeline.process(\\w*)")).find())
        .overridingErrorMessage(e).isTrue();
    return this;
  }

  public CodeAssert nestedEnd() {
    if (matched == null)
      throw new AssertionError("Did not start AST traversal.");
    String e = patternErrorMessage("nextPipeline-process", "nested");
    Assertions.assertThat(matcher.usePattern(deepPattern("(this.)??nextPipeline.process(\\w*)")).find())
        .overridingErrorMessage(e).isTrue();
    return this;
  }

  public CodeAssert hasVariable(String name, String value) {
    if (matched == null)
      throw new AssertionError("Did not start AST traversal.");
    String e = patternErrorMessage(name.concat(" = ").concat(value), "variable");
    Assertions.assertThat(matcher.usePattern(varPattern(name, value)).find()).overridingErrorMessage(e).isTrue();
    return this;
  }
}
