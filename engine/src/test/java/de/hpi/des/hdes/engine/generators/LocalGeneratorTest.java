package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.window.CWindow;
import de.hpi.des.hdes.engine.window.Time;

import org.junit.jupiter.api.Test;

public class LocalGeneratorTest {

        // @Test
        public void sourceSinkStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .toFile(new PrimitiveType[] { PrimitiveType.LONG }, 1000);
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
                CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(1).hasUnaryNodes(0).hasJoinNodes(0)
                                .hasSourcePipelines(1).hasSinkPipelines(1).hasUnaryPipelines(0).hasJoinPipelines(0)
                                .gotGenerated();
        }

        // @Test
        public void sourceFilterStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .filter(new PrimitiveType[] { PrimitiveType.LONG }, "v1 -> v1 > 1");
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
                CodeAssert.assertThat(pt, builder).hasSinkNodes(0).hasSourceNodes(1).hasUnaryNodes(1).hasJoinNodes(0)
                                .hasSourcePipelines(1).hasSinkPipelines(0).hasUnaryPipelines(1).hasJoinPipelines(0)
                                .gotGenerated().hasVariable("v1", "input.getLong()");
        }

        // @Test
        public void sourceFilterSinkStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .filter(new PrimitiveType[] { PrimitiveType.LONG }, "v1 -> v1 > 1")
                                .toFile(new PrimitiveType[] { PrimitiveType.LONG }, 1000);
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
                // Validates at topolgy of nodes
                CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(1).hasUnaryNodes(1).hasJoinNodes(0)
                                .hasSourcePipelines(1).hasSinkPipelines(1).hasUnaryPipelines(1).hasJoinPipelines(0)
                                .gotGenerated().hasVariable("v1", "input.getLong()");
        }

        // @Test
        public void sourceJoinStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                var stream = builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080);
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .join(stream, new PrimitiveType[] { PrimitiveType.LONG },
                                                new PrimitiveType[] { PrimitiveType.LONG }, 1, 2,
                                                CWindow.tumblingWindow(Time.seconds(1)))
                                .toFile(new PrimitiveType[] { PrimitiveType.LONG }, 1000);
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
                String id = pt.getPipelines().stream().filter(p -> p instanceof JoinPipeline).findFirst().get()
                                .getPipelineId();
                CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(2).hasUnaryNodes(0).hasJoinNodes(2)
                                .hasSourcePipelines(2).hasSinkPipelines(1).hasUnaryPipelines(0).hasJoinPipelines(1)
                                .gotGenerated().traverseAST(id).hasVariable("joinKeyExtractor", "e1 -> e1")
                                .hasVariable("joinKeyExtractor", "e2 -> e2");
        }

        @Test
        public void sourceAverageStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .average(new PrimitiveType[] { PrimitiveType.LONG }, 0, 1000)
                                .toFile(new PrimitiveType[] { PrimitiveType.LONG }, 1000);
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
        }

        // @Test
        public void sourceAJoinStreamTest() {
                VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
                var stream = builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080);
                builder.streamOfC(new PrimitiveType[] { PrimitiveType.LONG }, "host", 8080)
                                .ajoin(stream, new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
                                                new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, 0, 0,
                                                CWindow.tumblingWindow(Time.seconds(1)))
                                .toFile(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
                                                PrimitiveType.INT }, 1000);
                LocalGenerator generator = new LocalGenerator();
                PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
                generator.extend(pt.getPipelines());
                // TODO why is the AJoin node duplicated in the builder notes list?
                CodeAssert.assertThat(pt, builder).hasSinkNodes(1).hasSourceNodes(2).hasUnaryNodes(0).hasAJoinNodes(2)
                                .hasSourcePipelines(2).hasSinkPipelines(1).hasUnaryPipelines(0).hasAJoinPipelines(1)
                                .gotGenerated();
        }
}
