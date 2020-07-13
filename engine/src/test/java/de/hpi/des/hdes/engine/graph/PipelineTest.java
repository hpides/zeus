package de.hpi.des.hdes.engine.graph;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.node.NetworkSourceNode;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

public class PipelineTest {
  @Test
  public void networkSourcePipelineEquals() {
    PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.INT };
    NetworkSourcePipeline p1 = new NetworkSourcePipeline(new NetworkSourceNode(types, "192.168.198.172", 80));
    NetworkSourcePipeline p2 = new NetworkSourcePipeline(new NetworkSourceNode(types, "192.168.198.172", 80));
    assertThat(p1).isEqualTo(p2);
    NetworkSourcePipeline p3 = new NetworkSourcePipeline(new NetworkSourceNode(types, "192.168.198.172", 90));
    assertThat(p1).isNotEqualTo(p3);
  }

  @Test
  public void unaryPipelineEquals() {
    PrimitiveType[] types = new PrimitiveType[] { PrimitiveType.INT };
    VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
    builder.streamOfC(types, "192.168.172.199", 80).filter(types, "() -> true").map(new Tuple(types).get(0));
    PipelineTopology pt1 = PipelineTopology.pipelineTopologyOf(builder.build());
    VulcanoTopologyBuilder builder2 = new VulcanoTopologyBuilder();
    builder2.streamOfC(types, "192.168.172.199", 80).filter(types, "() -> true").map(new Tuple(types).get(0));
    PipelineTopology pt2 = PipelineTopology.pipelineTopologyOf(builder2.build());
    assertThat(pt1.getPipelines()).containsAll(pt2.getPipelines());
    VulcanoTopologyBuilder builder3 = new VulcanoTopologyBuilder();
    builder3.streamOfC(types, "192.168.172.199", 90).filter(types, "() -> true").map(new Tuple(types).get(0));
    PipelineTopology pt3 = PipelineTopology.pipelineTopologyOf(builder3.build());
    assertThat(pt3.getPipelines()).doesNotContainAnyElementsOf(pt1.getPipelines());
    VulcanoTopologyBuilder builder4 = new VulcanoTopologyBuilder();
    builder4.streamOfC(types, "192.168.172.199", 90).filter(types, "() -> false").map(new Tuple(types).get(0));
    PipelineTopology pt4 = PipelineTopology.pipelineTopologyOf(builder4.build());
    for (Pipeline p : pt3.getPipelines()) {
      if (p instanceof NetworkSourcePipeline) {
        assertThat(pt4.getPipelines()).contains(p);
      } else {
        assertThat(pt4.getPipelines()).doesNotContain(p);
      }
    }
  }
}