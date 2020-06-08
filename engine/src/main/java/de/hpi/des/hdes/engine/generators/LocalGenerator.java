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
import lombok.extern.slf4j.Slf4j;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.SourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryGenerationNode;

// TODO If there is a BinaryPipeline (No source), set at generation time for the preceding pipelines if they are left or right

@Slf4j
public class LocalGenerator implements PipelineVisitor {

    private final PipelineTopology pipelineTopology;
    private final StringWriter writer = new StringWriter();

    private String tempDirectory = "./engine/src/main/java/de/hpi/des/hdes/engine/temp/";

    @Getter
    private class JoinData {
        private String className;
        private String nextClassName;
        private long slide;
        private long length;
        private String leftImplementation;
        private String rightImplementation;

        public JoinData(final String className, final String nextClassName, final long slide, final long length,
                final String leftImplementation, final String rightImplementation) {
            this.className = className;
            this.nextClassName = nextClassName;
            this.slide = slide;
            this.length = length;
            this.leftImplementation = leftImplementation;
            this.rightImplementation = rightImplementation;
        }
    }

    @Getter
    private class AggregationData {
        private String className;
        private long slide;
        private long length;
        private String implementation;
        private String execution;

        public AggregationData(String className, long slide, long length, String implementation, String execution) {
            this.className = className;
            this.slide = slide;
            this.length = length;
            this.implementation = implementation;
            this.execution = execution;
        }
    }

    @Getter
    private class SourceData {
        private String className;
        private String nextPipelineClass;
        private String nextPipelineFunction;

        public SourceData(String className, String nextPipelineClass, String nextPipelineFunction) {
            this.className = className;
            this.nextPipelineClass = nextPipelineClass;
            this.nextPipelineFunction = nextPipelineFunction;
        }
    }

    public LocalGenerator(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
    }

    public static LocalGenerator build(final PipelineTopology pipelineTopology) {
        return new LocalGenerator(pipelineTopology);
    }

    public void extend(final PipelineTopology pipelineTopology) {
        for (Pipeline pipeline : pipelineTopology.getPipelines()) {
            pipeline.accept(this);
        }
    }

    @Override
    public void visit(UnaryPipeline unaryPipeline) {
        String execution = unaryPipeline.getChild().getPipelineId() + ".process(element);";
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
            template.execute(writer,
                    new AggregationData(unaryPipeline.getPipelineId(), 1000, 1000, implementation, execution))
                    // TODO: Set length and slide
                    .flush();
            implementation = writer.toString();
            Files.writeString(Paths.get(tempDirectory + unaryPipeline.getPipelineId() + ".java"), implementation);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }

    @Override
    public void visit(BinaryPipeline binaryPipeline) {
        String nextPipelineFunction = this.pipelineTopology.getChildProcessMethod(binaryPipeline,
                binaryPipeline.getChild());
        String leftImplementation = "";
        leftImplementation = binaryPipeline.getBinaryNode().getOperator().generate(leftImplementation,
                nextPipelineFunction, true);

        for (Node node : Lists.reverse(binaryPipeline.getLeftNodes())) {
            if (node instanceof UnaryGenerationNode) {
                leftImplementation = ((UnaryGenerationNode) node).getOperator().generate(leftImplementation);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", node.getClass()));
            }
        }

        String rightImplementation = "";
        rightImplementation = binaryPipeline.getBinaryNode().getOperator().generate(rightImplementation,
                nextPipelineFunction, false);

        for (Node node : Lists.reverse(binaryPipeline.getRightNodes())) {
            if (node instanceof UnaryGenerationNode) {
                rightImplementation = ((UnaryGenerationNode) node).getOperator().generate(rightImplementation);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", node.getClass()));
            }
        }

        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("JoinPipeline.java.mustache");
            String nextClassName = "";
            if (binaryPipeline.getChild() == null) {
                nextClassName = "TempSink";
            } else {
                nextClassName = binaryPipeline.getChild().getPipelineId();
            }
            template.execute(writer,
                    new JoinData(binaryPipeline.getPipelineId(), nextClassName, 1000, 1000, leftImplementation,
                            rightImplementation))
                    // TODO: Set length and slide
                    .flush();
            String implementation = writer.toString();
            Files.writeString(Paths.get(tempDirectory + binaryPipeline.getPipelineId() + ".java"), implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }

    @Override
    public void visit(SourcePipeline sourcePipeline) {
        String nextPipelineFunction = this.pipelineTopology.getChildProcessMethod(sourcePipeline,
                sourcePipeline.getChild());
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("Source.java.mustache");
            template.execute(writer, new SourceData(sourcePipeline.getPipelineId(),
                    sourcePipeline.getChild().getPipelineId(), nextPipelineFunction)).flush();
            String implementation = writer.toString();
            Files.writeString(Paths.get(tempDirectory + sourcePipeline.getPipelineId() + ".java"), implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }
}
