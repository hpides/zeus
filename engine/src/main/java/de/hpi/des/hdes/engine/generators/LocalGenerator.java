package de.hpi.des.hdes.engine.generators;

import com.github.mustachejava.Mustache;
import com.google.common.collect.Lists;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.io.StringWriter;
import lombok.extern.slf4j.Slf4j;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.templatedata.*;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
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

    public void extend(final List<Pipeline> pipelines) {
        for (Pipeline pipeline : pipelines) {
            pipeline.accept(this);
        }
    }

    @Override
    public void visit(UnaryPipeline unaryPipeline) {
        String implementation = "";

        for (Node node : Lists.reverse(unaryPipeline.getNodes())) {
            if (node instanceof UnaryGenerationNode) {
                implementation = implementation
                        .concat(((UnaryGenerationNode) node).getOperator().generate(unaryPipeline));
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        implementation = implementation.concat(unaryPipeline.getWriteout("input"));

        try {
            if (unaryPipeline.hasChild() && !(unaryPipeline.getChild() instanceof SinkPipeline)) {
                // TODO
            } else {
                Mustache template = MustacheFactorySingleton.getInstance().compile("EmptyPipeline.java.mustache");
                template.execute(writer,
                        new EmptyPipelineData(unaryPipeline.getPipelineId(), implementation, unaryPipeline.getChild(),
                                unaryPipeline.getInterfaces(), unaryPipeline.getVariables(),
                                unaryPipeline.getInputTypes(), unaryPipeline.getOutputTypes()))
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
        String leftImplementation = "";
        String rightImplementation = "";

        for (Node node : Lists.reverse(joinPipeline.getLeftNodes())) {
            if (node instanceof UnaryGenerationNode) {
                leftImplementation = leftImplementation.concat(
                        ((UnaryGeneratable) ((UnaryGenerationNode) node).getOperator()).generate(joinPipeline, false));
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        for (Node node : Lists.reverse(joinPipeline.getRightNodes())) {
            if (node instanceof UnaryGenerationNode) {
                rightImplementation = rightImplementation.concat(
                        ((UnaryGeneratable) ((UnaryGenerationNode) node).getOperator()).generate(joinPipeline, true));
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }
        String leftVariableName = joinPipeline.getVariableAtIndex(
                ((JoinGenerator) joinPipeline.getBinaryNode().getOperator()).getKeyPositionLeft(), false).getVarName();
        String rightVariableName = joinPipeline.getVariableAtIndex(
                ((JoinGenerator) joinPipeline.getBinaryNode().getOperator()).getKeyPositionRight(), true).getVarName();
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("JoinPipeline.java.mustache");
            FileWriter out = new FileWriter(Paths
                    .get(DirectoryHelper.getTempDirectoryPath() + joinPipeline.getPipelineId() + ".java").toFile());
            JoinGenerator operator = (JoinGenerator) joinPipeline.getBinaryNode().getOperator();
            template.execute(out,
                    new JoinData(joinPipeline.getPipelineId(), operator.getLeftTypes(), operator.getLeftTypes(),
                            operator.getKeyPositionLeft(), operator.getKeyPositionRight(), operator.getWindow(),
                            joinPipeline.getInterfaces(), joinPipeline.getVariables(), joinPipeline.getJoinVariables(),
                            leftImplementation, rightImplementation, leftVariableName, rightVariableName,
                            joinPipeline.getWriteout("leftInput", false), joinPipeline.getWriteout("rightInput", true)))
                    .flush();
        } catch (IOException e) {
            log.error("Write out error: {}", e);
        }
    }

    @Override
    public void visit(AJoinPipeline ajoinPipeline) {
        String leftImplementation = "";
        String rightImplementation = "";

        for (Node node : Lists.reverse(ajoinPipeline.getLeftNodes())) {
            if (node instanceof UnaryGenerationNode) {
                leftImplementation = leftImplementation.concat(
                        ((UnaryGeneratable) ((UnaryGenerationNode) node).getOperator()).generate(ajoinPipeline, false));
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }

        for (Node node : Lists.reverse(ajoinPipeline.getRightNodes())) {
            if (node instanceof UnaryGenerationNode) {
                rightImplementation = rightImplementation.concat(
                        ((UnaryGeneratable) ((UnaryGenerationNode) node).getOperator()).generate(ajoinPipeline, true));
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }
        String leftVariableName = ajoinPipeline
                .getVariableAtIndex(((AJoinGenerator) ajoinPipeline.getBinaryNode().getOperator()).getKeyPositionLeft(),
                        false)
                .getVarName();
        String rightVariableName = ajoinPipeline
                .getVariableAtIndex(
                        ((AJoinGenerator) ajoinPipeline.getBinaryNode().getOperator()).getKeyPositionRight(), true)
                .getVarName();
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("AJoinPipeline.java.mustache");
            FileWriter out = new FileWriter(Paths
                    .get(DirectoryHelper.getTempDirectoryPath() + ajoinPipeline.getPipelineId() + ".java").toFile());
            AJoinGenerator operator = (AJoinGenerator) ajoinPipeline.getBinaryNode().getOperator();
            template.execute(out,
                    new AJoinData(ajoinPipeline.getPipelineId(), operator.getLeftTypes(), operator.getLeftTypes(),
                            operator.getKeyPositionLeft(), operator.getKeyPositionRight(), operator.getWindow(),
                            ajoinPipeline.getInterfaces(), ajoinPipeline.getVariables(),
                            ajoinPipeline.getJoinVariables(), leftImplementation, rightImplementation, leftVariableName,
                            rightVariableName, ajoinPipeline.getWriteout("leftInput", false),
                            ajoinPipeline.getWriteout("rightInput", true)))
                    .flush();
        } catch (IOException e) {
            log.error("Write out error: {}", e);
        }
    }

    @Override
    public void visit(NetworkSourcePipeline sourcePipeline) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("NetworkSource.java.mustache");
            template.execute(writer,
                    new NetworkSourceData(sourcePipeline.getPipelineId(),
                            (sourcePipeline.getInputTupleLength() + 9) + "", sourcePipeline.getSourceNode().getHost(),
                            sourcePipeline.getSourceNode().getPort() + "", Dispatcher.TUPLES_PER_VECTOR() + ""))
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
                implementation = ((UnaryGenerationNode) node).getOperator().generate(aggregationPipeline);
            } else {
                System.err.println(String.format("Node %s not implemented for code generation.", Node.class));
            }
        }
        AggregateGenerator operator = (AggregateGenerator) aggregationPipeline.getAggregationGenerationNode()
                .getOperator();
        String variableName = aggregationPipeline.getVariableAtIndex(operator.getAggregateValueIndex()).getVarName();
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("AggregationPipeline.java.mustache");
            template.execute(writer,
                    new AggregationData(aggregationPipeline.getPipelineId(),
                            aggregationPipeline.getAggregationGenerationNode().getInputTypes(),
                            operator.getAggregateValueIndex(), variableName, operator.getAggregateFunction(),
                            aggregationPipeline.getInterfaces(), aggregationPipeline.getVariables(), implementation,
                            operator.getWindowLength()))
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
                            (fileSinkPipeline.getInputTupleLength() + 9) + "", fileSinkPipeline.getWriteEveryX() + "",
                            Dispatcher.TUPLES_PER_VECTOR() + "", Dispatcher.TUPLES_PER_READ_VECTOR() + ""))
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
