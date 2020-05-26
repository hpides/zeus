package de.hpi.des.hdes.engine.generators;

import de.hpi.des.hdes.engine.cstream.CStream;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Stack;

import com.google.common.eventbus.AllowConcurrentEvents;

import java.util.ArrayList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import de.hpi.des.hdes.engine.astream.AStream;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopologyBuilder;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.vulcano.Topology;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;

public class LocalGeneratorTest {

    @Test
    public void generateForModuloFilter() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        final List<Integer> listS1 = new ArrayList<>();
        final ListSource<Integer> source = new ListSource<>(listS1, new WatermarkGenerator<>(-1, -1), e -> e);
        builder.streamOf(source);
        FilterGenerator filter = new FilterGenerator("element % 4 == 0");
        UnaryGenerationNode node = new UnaryGenerationNode(filter);
        builder.addGraphNode(builder.getNodes().get(0), node);
        LocalGenerator generator = new LocalGenerator(new PipelineTopology(new ArrayList<Pipeline>()));
        generator.extend(PipelineTopologyBuilder.pipelineTopologyOf(builder.build()));
        // try {
        // String result = Files.readString(Paths.get(uuid + ".java"));
        // assertEquals(
        // String.format("class %s {void pipeline(AData<> element) {if ( element %% 4 ==
        // 0 ) { }}}", uuid, ""),
        // result.replaceAll("( ){2,}|\n|\r", ""));
        // } catch (IOException e) {
        // System.out.println(e.getMessage());
        // System.exit(1);
        // }
    }

    @Test
    public void testCStream() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        CStream stream2 = builder.streamOfC(new BufferedSource() {

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
        }).map("").map("").map("").map("").map("").map("");
        VulcanoTopologyBuilder builded = builder.streamOfC(new BufferedSource() {

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
        }).filter("").join(stream2).aggregate().to();
        PipelineTopology tmep = PipelineTopologyBuilder.pipelineTopologyOf(builded.build());
        System.out.println("Done");

    }

    @Test
    public void sourceJoinStreamTest() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        CStream stream2 = builder.streamOfC(new BufferedSource() {

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
        }).filter("element % 2 == 0");
        builder.streamOfC(new BufferedSource() {

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
        }).filter("element < 100").join(stream2);
        LocalGenerator generator = new LocalGenerator(new PipelineTopology(new ArrayList<Pipeline>()));
        generator.extend(PipelineTopologyBuilder.pipelineTopologyOf(builder.build()));
    }
}
