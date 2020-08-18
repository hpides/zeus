package de.hpi.des.hdes.engine.graph.pipeline;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import de.hpi.des.hdes.engine.cstream.CStream;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.AJoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.NetworkSourceNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.window.CWindow;
import de.hpi.des.hdes.engine.window.Time;

public class BinaryPipelineTest {
  @Test
  public void correctIsLeftForPipeline() {
    VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

    CStream auctionSource = builder.streamOfC(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
        "172.0.0.1", 50);

    CStream bidSource = builder.streamOfC(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
        "172.0.0.1", 51);

    auctionSource
        .ajoin(bidSource,
            new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
            new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT }, 1, 0,
            CWindow.tumblingWindow(Time.seconds(1)))
        .toFile(new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
            1000);
    ;

    AJoinGenerationNode ajoinNode = null;
    NetworkSourceNode auction = null;
    NetworkSourceNode bid = null;
    for (Node node : builder.getNodes()) {
      if (node instanceof AJoinGenerationNode) {
        ajoinNode = (AJoinGenerationNode) node;
      }
      if (node instanceof NetworkSourceNode) {
        if (((NetworkSourceNode) node).getPort() == 51) {
          bid = (NetworkSourceNode) node;
        } else {
          auction = (NetworkSourceNode) node;
        }
      }
    }
    if (ajoinNode != null && auction != null && bid != null) {
      assertThat(new AJoinPipeline(ajoinNode).isLeft(auction)).isTrue();
      assertThat(new AJoinPipeline(ajoinNode).isLeft(bid)).isFalse();
    } else {
      assertThat(true).overridingErrorMessage("Could not find all required nodes.").isFalse();
    }
    PipelineTopology ptl = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
    ptl.getPipelines();
  }

  @Test
  public void correctIsLeftForNodeinPipeline() {
    VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

    CStream auctionSource = builder.streamOfC(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
        "172.0.0.1", 50);

    CStream bidSource = builder.streamOfC(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
        "172.0.0.1", 51);

    CStream aJoin = auctionSource
        .map(new Tuple(PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT).get(0))
        .ajoin(bidSource,
            new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
            new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT }, 1, 0,
            CWindow.tumblingWindow(Time.seconds(1)));

    AJoinGenerationNode ajoinNode = null;
    NetworkSourceNode auction = null;
    NetworkSourceNode bid = null;
    UnaryGenerationNode map = null;
    for (Node node : builder.getNodes()) {
      if (node instanceof AJoinGenerationNode) {
        ajoinNode = (AJoinGenerationNode) node;
      }
      if (node instanceof NetworkSourceNode) {
        if (((NetworkSourceNode) node).getPort() == 51) {
          bid = (NetworkSourceNode) node;
        } else {
          auction = (NetworkSourceNode) node;
        }
      }
      if (node instanceof UnaryGenerationNode) {
        map = (UnaryGenerationNode) node;
      }
    }
    if (ajoinNode != null && auction != null && bid != null) {
      assertThat(new AJoinPipeline(ajoinNode).isLeft(auction)).isTrue();
      assertThat(new AJoinPipeline(ajoinNode).isLeft(map)).isTrue();
      assertThat(new AJoinPipeline(ajoinNode).isLeft(bid)).isFalse();
    } else {
      assertThat(true).overridingErrorMessage("Could not find all required nodes.").isFalse();
    }
  }

  @Test
  public void correctIsLeftForNodeinRightPipeline() {
    VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();

    CStream auctionSource = builder.streamOfC(
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT },
        "172.0.0.1", 50);

    CStream bidSource = builder
        .streamOfC(new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
            "172.0.0.1", 51)
        .map(new Tuple(PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT).get(0));

    CStream aJoin = auctionSource.ajoin(bidSource,
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT },
        new PrimitiveType[] { PrimitiveType.LONG, PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT }, 1, 0,
        CWindow.tumblingWindow(Time.seconds(1)));

    AJoinGenerationNode ajoinNode = null;
    NetworkSourceNode auction = null;
    NetworkSourceNode bid = null;
    UnaryGenerationNode map = null;
    for (Node node : builder.getNodes()) {
      if (node instanceof AJoinGenerationNode) {
        ajoinNode = (AJoinGenerationNode) node;
      }
      if (node instanceof NetworkSourceNode) {
        if (((NetworkSourceNode) node).getPort() == 51) {
          bid = (NetworkSourceNode) node;
        } else {
          auction = (NetworkSourceNode) node;
        }
      }
      if (node instanceof UnaryGenerationNode) {
        map = (UnaryGenerationNode) node;
      }
    }
    if (ajoinNode != null && auction != null && bid != null) {
      assertThat(new AJoinPipeline(ajoinNode).isLeft(auction)).isTrue();
      assertThat(new AJoinPipeline(ajoinNode).isLeft(map)).isFalse();
      assertThat(new AJoinPipeline(ajoinNode).isLeft(bid)).isFalse();
    } else {
      assertThat(true).overridingErrorMessage("Could not find all required nodes.").isFalse();
    }
  }
}