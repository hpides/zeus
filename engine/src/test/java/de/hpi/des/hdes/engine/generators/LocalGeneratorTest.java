package de.hpi.des.hdes.engine.generators;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Stack;
import java.util.ArrayList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.graph.UnaryGenerationNode;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.stream.AStream;

public class LocalGeneratorTest {
    @Test
    public void generateForModuloFilter() {
        TopologyBuilder builder = new TopologyBuilder();
        final List<Integer> listS1 = new ArrayList<>();
        final ListSource<Integer> source = new ListSource<>(listS1, new WatermarkGenerator<>(-1, -1), e -> e);
        AStream<Integer> aStream = builder.streamOf(source);
        FilterGenerator<Integer> filter = new FilterGenerator<Integer>("element % 4 == 0");
        UnaryGenerationNode<Integer, Integer> node = new UnaryGenerationNode<Integer, Integer>(filter);
        builder.addGraphNode(builder.getNodes().get(0), node);
        LocalGenerator generator = new LocalGenerator(new Topology());
        Stack<String> pipelineUuids = generator.build(builder.build());
        String uuid = pipelineUuids.pop();
        try {
            String result = Files.readString(Paths.get(uuid + ".java"));
            assertEquals(
                    String.format("class %s {void pipeline(AData<> element) {if ( element %% 4 == 0 ) { }}}", uuid, ""),
                    result.replaceAll("( ){2,}|\n|\r", ""));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
}
