package de.hpi.des.hdes.engine.generators;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

import com.github.mustachejava.Mustache;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.StringWriter;
import lombok.Getter;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryGenerationNode;

public class LocalGenerator implements PipelineVisitor {

    private final PipelineTopology pipelineTopology;
    String implementation = "";

    private final StringWriter writer = new StringWriter();

    @Getter
    private class JoinData {
        private String className;
        private long slide;
        private long length;
        private String leftImplementation;
        private String rightImplementation;

        public JoinData(String className, long slide, long length, String leftImplementation,
                String rightImplementation) {
            this.className = className;
            this.slide = slide;
            this.length = length;
            this.leftImplementation = leftImplementation;
            this.rightImplementation = rightImplementation;
        }
    }

    @Getter
    private class AggregationData {
        private String className;
        private String implementation;
        private String execution;

        public AggregationData(String className, String implementation, String execution) {
            this.className = className;
            this.implementation = implementation;
            this.execution = execution;
        }
    }

    public LocalGenerator(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
    }

    public static LocalGenerator build(final PipelineTopology queryTopology) {
        return new LocalGenerator(queryTopology);
    }

    public void extend(final PipelineTopology queryTopology) {
        for (Pipeline pipeline : queryTopology.getPipelines()) {
            pipeline.accept(this);
        }
    }

    @Override
    public void visit(UnaryPipeline unaryPipeline) {
        String execution = unaryPipeline.getChild().getPipelineId() + ".process(AData<> element);";
        String implementation = "";
        for (Node node : Lists.reverse(unaryPipeline.getNodes())) {
            if (node instanceof UnaryGenerationNode) {
                implementation = ((UnaryGenerationNode) node).getOperator().generate(implementation);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("AggregationPipeline.java.mustache");
            template.execute(writer, new AggregationData(unaryPipeline.getPipelineId(), implementation, execution))
                    .flush();
            implementation = writer.toString();
            Files.writeString(Paths.get(unaryPipeline.getPipelineId() + ".java"), implementation);
        } catch (IOException e) {
            System.exit(1);
        }

    }

    @Override
    public void visit(BinaryPipeline binaryPipeline) {
        String leftImplementation = binaryPipeline.getChild().getPipelineId() + ".process(AData<> element);";
        for (Node node : Lists.reverse(binaryPipeline.getLeftNodes())) {
            if (node instanceof UnaryGenerationNode) {
                leftImplementation = ((UnaryGenerationNode) node).getOperator().generate(leftImplementation);
            } else if (node instanceof BinaryGenerationNode) {
                leftImplementation = ((BinaryGenerationNode) node).getOperator().generate(leftImplementation, true);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        String rightImplementation = binaryPipeline.getChild().getPipelineId() + ".process(AData<> element);";
        for (Node node : Lists.reverse(binaryPipeline.getRightNodes())) {
            if (node instanceof UnaryGenerationNode) {
                rightImplementation = ((UnaryGenerationNode) node).getOperator().generate(rightImplementation);
            } else if (node instanceof BinaryGenerationNode) {
                rightImplementation = ((BinaryGenerationNode) node).getOperator().generate(rightImplementation, false);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("JoinPipeline.java.mustache");
            template.execute(writer,
                    new JoinData(binaryPipeline.getPipelineId(), 1000, 1000, leftImplementation, rightImplementation))
                    // TODO: Set length and slide
                    .flush();
            implementation = writer.toString();
            Files.writeString(Paths.get(binaryPipeline.getPipelineId() + ".java"), implementation);
        } catch (IOException e) {
            System.exit(1);
        }
    }
}
