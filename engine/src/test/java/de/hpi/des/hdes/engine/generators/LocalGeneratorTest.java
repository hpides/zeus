package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSink;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;

public class LocalGeneratorTest {

    private BufferedSource source = new BufferedSource(){
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
        @Override
        public void shutdown() {
            // TODO Auto-generated method stub   
        }
        @Override
        public Buffer getInputBuffer() {
            // TODO Auto-generated method stub
            return null;
        }
    };

    private BufferedSink sink = new BufferedSink(){
        @Override
        public void run() {
            // TODO Auto-generated method stub
        }
        @Override
        public void shutdown() {
            // TODO Auto-generated method stub   
        }
        @Override
        public Buffer getOutputBuffer() {
            // TODO Auto-generated method stub
            return null;
        }
    };
    
    @Test
    public void sourceSinkStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(source).to(sink);
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        CodeAssert.assertThat(pt, builder)
            .hasSinkNodes(1)
            .hasSourceNodes(1)
            .hasUnaryNodes(0)
            .hasBinaryNodes(0)
            .hasSourcePipelines(1)
            .hasSinkPipelines(1)
            .hasUnaryPipelines(0)
            .hasBinaryPipelines(0)
            .gotGenerated()
            .isConnected();
    }

    @Test
    public void sourceFilterStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(source).filter("event.getData() > 2");
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        CodeAssert.assertThat(pt, builder)
            .hasSinkNodes(0)
            .hasSourceNodes(1)
            .hasUnaryNodes(1)
            .hasBinaryNodes(0)
            .hasSourcePipelines(1)
            .hasSinkPipelines(0)
            .hasUnaryPipelines(1)
            .hasBinaryPipelines(0)
            .gotGenerated()
            .isConnected()
            .traverseAST("event.getData() > 2");
    }

    @Test
    public void sourceFilterSinkStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(source).filter("event.getData() > 2").to(sink);
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        // Validates at topolgy of nodes
        CodeAssert.assertThat(pt, builder)
            .hasSinkNodes(1)
            .hasSourceNodes(1)
            .hasUnaryNodes(1)
            .hasBinaryNodes(0)
            .hasSourcePipelines(1)
            .hasSinkPipelines(1)
            .hasUnaryPipelines(1)
            .hasBinaryPipelines(0)
            .gotGenerated()
            .isConnected()
            .traverseAST("event.getData() > 2")
            .endsPipeline();
    }

    @Test
    public void sourceJoinStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        var stream = builder.streamOfC(source);
        builder.streamOfC(source).join(stream, "e1 -> e1", "e2 -> e2", "(l,r) -> l+r");
        LocalGenerator generator = new LocalGenerator(new PipelineTopology());
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        generator.extend(pt);
        String id = pt.getPipelines().stream().filter(p -> p instanceof BinaryPipeline).findFirst().get().getPipelineId();
        CodeAssert.assertThat(pt, builder)
            .hasSinkNodes(0)
            .hasSourceNodes(2)
            .hasUnaryNodes(0)
            .hasBinaryNodes(2)
            .hasSourcePipelines(2)
            .hasSinkPipelines(0)
            .hasUnaryPipelines(0)
            .hasBinaryPipelines(1)
            .gotGenerated()
            .isConnected()
            .traverseAST(id)
            .hasVariable("joinKeyExtractor", "e1 -> e1")
            .hasVariable("joinKeyExtractor", "e2 -> e2")
            .hasVariable("joinMapper", "(l,r) -> l+r");
    }
}