package de.hpi.des.hdes.engine.graph;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.window.GlobalTimeWindow;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.operation.StreamJoin;
import de.hpi.des.hdes.engine.operation.StreamMap;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopologyTest {

  @Test
  void shouldTopologicallySort() {
    List<Integer> inputTuples1 = List.of(1, 2, 3, 4, 5);
    Source<Integer> source1 = new ListSource<>(inputTuples1);
    SourceNode<Integer> sourceNode1 = new SourceNode<>(source1);

    List<Integer> inputTuples2 = List.of(1, 2, 3, 4, 5);
    Source<Integer> source2 = new ListSource<>(inputTuples2);
    SourceNode<Integer> sourceNode2 = new SourceNode<>(source2);

    StreamMap<Integer, Integer> map1 = new StreamMap<>(x -> x + 1);
    UnaryOperationNode<Integer, Integer> mapNode1 = new UnaryOperationNode<>(map1);

    StreamMap<Integer, Integer> map2 = new StreamMap<>(x -> x + 2);
    UnaryOperationNode<Integer, Integer> mapNode2 = new UnaryOperationNode<>(map2);

    StreamJoin<Integer, Integer, String, GlobalTimeWindow> join = new StreamJoin<>(
        (x, y) -> "Matches: " + x + y,
        Integer::equals,
        GlobalWindow.create());

    BinaryOperationNode<Integer, Integer, String> joinNode = new BinaryOperationNode<>(join);

    /*
     * sourceNode1 --> mapNode1
     *                           joinNode --> MapNode2
     * sourceNode2 ----------->
     */

    sourceNode1.addChild(mapNode1);
    mapNode1.addChild(joinNode);
    sourceNode2.addChild(joinNode);
    joinNode.addChild(mapNode2);

    List<Node> unorderedNodes = List.of(
        sourceNode1,
        sourceNode2,
        mapNode1,
        mapNode2,
        joinNode
    );

    Topology topology = new Topology(unorderedNodes);
    final List<Node> nodes = topology.getTopologicalOrdering();

    assertThat(nodes.indexOf(sourceNode1)).isLessThan(nodes.indexOf(mapNode1));
    assertThat(nodes.indexOf(mapNode1)).isLessThan(nodes.indexOf(joinNode));
    assertThat(nodes.indexOf(sourceNode2)).isLessThan(nodes.indexOf(joinNode));
    assertThat(nodes.indexOf(joinNode)).isLessThan(nodes.indexOf(mapNode2));
  }

}
