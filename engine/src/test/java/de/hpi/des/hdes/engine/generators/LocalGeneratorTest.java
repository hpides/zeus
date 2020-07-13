package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSink;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;

public class LocalGeneratorTest {

    private BufferedSource source = new BufferedSource() {

        @Override
        public Buffer getInputBuffer() {
            // TODO Auto-generated method stub
            return null;
        }
    };

    private BufferedSink sink = new BufferedSink() {

        @Override
        public Buffer getOutputBuffer() {
            // TODO Auto-generated method stub
            return null;
        }
    };

    @Test
    public void sourceSinkStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source).to(sink,
                new PrimitiveType[] { PrimitiveType.LONG });
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(1).hasUnaryNodes(0).hasBinaryNodes(0)
                .hasSourcePipelines(1).hasSinkPipelines(1).hasUnaryPipelines(0).hasBinaryPipelines(0).gotGenerated()
                .isConnected();
    }

    // @Test
    public void sourceFilterStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source)
                .filter(new PrimitiveType[] { PrimitiveType.LONG }, "v1 -> v1 > 1");
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        CodeAssert.assertThat(pt, builder).hasSinkNodes(0).hasSourceNodes(1).hasUnaryNodes(1).hasBinaryNodes(0)
                .hasSourcePipelines(1).hasSinkPipelines(0).hasUnaryPipelines(1).hasBinaryPipelines(0).gotGenerated()
                .isConnected().hasVariable("v1", "input.getLong()");
    }

    // @Test
    public void sourceFilterSinkStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source)
                .filter(new PrimitiveType[] { PrimitiveType.LONG }, "v1 -> v1 > 1")
                .to(sink, new PrimitiveType[] { PrimitiveType.LONG });
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        // Validates at topolgy of nodes
        CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(1).hasUnaryNodes(1).hasBinaryNodes(0)
                .hasSourcePipelines(1).hasSinkPipelines(1).hasUnaryPipelines(1).hasBinaryPipelines(0).gotGenerated()
                .isConnected().hasVariable("v1", "input.getLong()");
    }

    @Test
    public void sourceJoinStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        var stream = builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source).join(stream,
                new PrimitiveType[] { PrimitiveType.LONG }, new PrimitiveType[] { PrimitiveType.LONG }, 1, 2,
                "(l,r) -> l+r");
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        String id = pt.getPipelines().stream().filter(p -> p instanceof JoinPipeline).findFirst().get().getPipelineId();
        CodeAssert.assertThat(pt, builder).hasSinkNodes(0).hasSourceNodes(2).hasUnaryNodes(0).hasBinaryNodes(2)
                .hasSourcePipelines(2).hasSinkPipelines(0).hasUnaryPipelines(0).hasBinaryPipelines(1).gotGenerated()
                .isConnected().traverseAST(id).hasVariable("joinKeyExtractor", "e1 -> e1")
                .hasVariable("joinKeyExtractor", "e2 -> e2").hasVariable("joinMapper", "(l,r) -> l+r");
    }

    @Test
    public void sourceAverageStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source)
                .average(new PrimitiveType[] { PrimitiveType.LONG }, 0)
                .to(sink, new PrimitiveType[] { PrimitiveType.LONG });
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
    }

    @Test
    public void sourceAJoinStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        var stream = builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source);
        builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, source)
                .ajoin(stream, new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
                        new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, 0, 0)
                .to(sink, new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
                        PrimitiveType.INT });
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
    }
}
