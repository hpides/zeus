package de.hpi.des.hdes.engine.generators;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

import com.github.mustachejava.Mustache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.StringWriter;
import lombok.Getter;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;

public class LocalGenerator {

    private final PipelineTopology pipelineTopology;
    private String implementation = "";

    private final StringWriter writer = new StringWriter();

    @Getter
    private class SourceData {
        private String className;
        private String implementation;

        public SourceData(String uuid, String implementation) {
            this.className = uuid;
            this.implementation = implementation;
        }
    }

    public LocalGenerator(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
    }

    public void build(final PipelineTopology queryTopology) {
        for (Pipeline pipeline : queryTopology.getPipelines()) {
            this.buildPipeline(pipeline);
        }
    }

    public void buildPipeline(Pipeline pipeline) {

    }

    /*
     * TODO do this for pipeline topology public void build(final Query query) {
     * this.build(query.getTopology()); }
     */

    /*
     * See if this can be reused.
     * 
     * @Override public <OUT> void visit(SourceNode<OUT> sourceNode) { String uuid =
     * "c".concat(UUID.randomUUID().toString().replaceAll("-", "")); try { Mustache
     * template =
     * MustacheFactorySingleton.getInstance().compile("Source.java.mustache");
     * template.execute(writer, new SourceData(uuid, implementation)).flush();
     * implementation = writer.toString(); Files.writeString(Paths.get(uuid +
     * ".java"), implementation); this.uuids.push(uuid); } catch (IOException e) {
     * System.exit(1); } }
     * 
     * @Override public <IN, OUT> void visit(UnaryGenerationNode<IN, OUT>
     * unaryGenerationNode) { implementation =
     * unaryGenerationNode.getOperator().generate(implementation); }
     * 
     * @Override public <IN1, IN2, OUT> void visit(BinaryGenerationNode<IN1, IN2,
     * OUT> binaryGenerationNode) { final JoinGenerator<IN1, IN2, OUT> operator =
     * binaryGenerationNode.getOperator(); final Iterator<Node> parents =
     * binaryGenerationNode.getParents().iterator(); final Node parentNode1 =
     * parents.next(); final Node parentNode2 = parents.next();
     * 
     * System.err.println(String.format("Node from type %s is not known.",
     * node.getClass())); }
     */
}
