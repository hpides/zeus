package de.hpi.des.hdes.engine.generators;

import com.github.mustachejava.Mustache;
import com.google.common.collect.Lists;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.StringWriter;
import lombok.extern.slf4j.Slf4j;
import de.hpi.des.hdes.engine.generators.templatedata.*;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.AJoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.FileSinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.AggregationPipeline;

// TODO If there is a JoinPipeline (No source), set at generation time for the preceding pipelines if they are left or right

@Slf4j
public class LocalGenerator extends PipelineVisitor {
    private final StringWriter writer = new StringWriter();

    public void extend(final PipelineTopology pipelineTopology) {
        for (Pipeline pipeline : pipelineTopology.getPipelines()) {
            pipeline.accept(this);
        }
    }

    @Override
    public void visit(UnaryPipeline unaryPipeline) {
        String implementation = unaryPipeline.hasChild()
                ? unaryPipeline.getWriteout("input").concat("dispatcher.write(\"").concat(unaryPipeline.getPipelineId())
                        .concat("\", output);")
                : "";

        for (Node node : Lists.reverse(unaryPipeline.getNodes())) {
            if (node instanceof UnaryGenerationNode) {
                implementation = ((UnaryGenerationNode) node).getOperator().generate(unaryPipeline, implementation);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        try {
            if (unaryPipeline.hasChild() && !(unaryPipeline.getChild() instanceof SinkPipeline)) {
                // TODO
            } else {
                Mustache template = MustacheFactorySingleton.getInstance().compile("EmptyPipeline.java.mustache");
                template.execute(writer,
                        new EmptyPipelineData(unaryPipeline.getPipelineId(), implementation, unaryPipeline.hasChild(),
                                unaryPipeline.getChild(), unaryPipeline.getInterfaces(), unaryPipeline.getVariables(),
                                unaryPipeline.getOutputTypes()))
                        .flush();
            }
            implementation = writer.toString();
            Files.writeString(
                    Paths.get(DirectoryHelper.getTempDirectoryPath() + unaryPipeline.getPipelineId() + ".java"),
                    implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }

    @Override
    public void visit(JoinPipeline joinPipeline) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("JoinPipeline.java.mustache");
            FileWriter out = new FileWriter(Paths
                    .get(DirectoryHelper.getTempDirectoryPath() + joinPipeline.getPipelineId() + ".java").toFile());
            JoinGenerator operator = (JoinGenerator) joinPipeline.getBinaryNode().getOperator();
            template.execute(out,
                    new JoinData(joinPipeline.getPipelineId(), operator.getLeftTypes(), operator.getLeftTypes(),
                            operator.getKeyPositionLeft(), operator.getKeyPositionRight(), operator.getWindowLength()))
                    .flush();
        } catch (IOException e) {
            log.error("Write out error: {}", e);
        }
    }

    @Override
    public void visit(AJoinPipeline aJoinPipeline) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("AJoinPipeline.java.mustache");
            FileWriter out = new FileWriter(Paths
                    .get(DirectoryHelper.getTempDirectoryPath() + aJoinPipeline.getPipelineId() + ".java").toFile());
            AJoinGenerator operator = (AJoinGenerator) aJoinPipeline.getBinaryNode().getOperator();
            template.execute(out,
                    new AJoinData(aJoinPipeline.getPipelineId(), operator.getLeftTypes(), operator.getLeftTypes(),
                            operator.getKeyPositionLeft(), operator.getKeyPositionRight(), operator.getWindowLength()))
                    .flush();
        } catch (IOException e) {
            log.error("Write out error: {}", e);
        }
    }

    @Override
    public void visit(NetworkSourcePipeline sourcePipeline) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("NetworkSource.java.mustache");
            template.execute(writer, new NetworkSourceData(sourcePipeline.getPipelineId(),
                    (sourcePipeline.getInputTupleLength() + 9) + "",
                    sourcePipeline.getSourceNode().getHost().toString(), sourcePipeline.getSourceNode().getHost()))
                    .flush();
            String implementation = writer.toString();
            Files.writeString(
                    Paths.get(DirectoryHelper.getTempDirectoryPath() + sourcePipeline.getPipelineId() + ".java"),
                    implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }

    @Override
    public void visit(AggregationPipeline aggregationPipeline) {
        String implementation = "";
        for (Node node : Lists.reverse(aggregationPipeline.getNodes())) {
            if (node instanceof UnaryGenerationNode) {
                implementation = ((UnaryGenerationNode) node).getOperator().generate(aggregationPipeline,
                        implementation);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("AggregationPipeline.java.mustache");
            AggregateGenerator operator = (AggregateGenerator) aggregationPipeline.getAggregationGenerationNode()
                    .getOperator();
            template.execute(writer,
                    new AggregationData(aggregationPipeline.getPipelineId(),
                            aggregationPipeline.getAggregationGenerationNode().getInputTypes(),
                            operator.getAggregateValueIndex(), operator.getAggregateFunction(),
                            aggregationPipeline.getInterfaces(), aggregationPipeline.getVariables(), implementation))
                    .flush();
            implementation = writer.toString();
            Files.writeString(
                    Paths.get(DirectoryHelper.getTempDirectoryPath() + aggregationPipeline.getPipelineId() + ".java"),
                    implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }

    @Override
    public void visit(FileSinkPipeline fileSinkPipeline) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("FileSink.java.mustache");
            template.execute(writer,
                    new FileSinkData(fileSinkPipeline.getPipelineId(),
                            (fileSinkPipeline.getInputTupleLength() + 9) + "", fileSinkPipeline.getWriteEveryX() + ""))
                    .flush();
            String implementation = writer.toString();
            Files.writeString(
                    Paths.get(DirectoryHelper.getTempDirectoryPath() + fileSinkPipeline.getPipelineId() + ".java"),
                    implementation);
            writer.getBuffer().setLength(0);
        } catch (IOException e) {
            log.error("Compile Error: {}", e);
        }
    }
}
