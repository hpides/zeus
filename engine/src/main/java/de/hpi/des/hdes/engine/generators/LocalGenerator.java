package de.hpi.des.hdes.engine.generators;

import java.util.List;
import java.util.UUID;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.graph.SourceNode;
import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.graph.UnaryGenerationNode;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;

public class LocalGenerator implements NodeVisitor {

    private final Topology topology;
    private String query;

    public LocalGenerator(final Topology topology) {
        this.topology = topology;
        try {
            String query = Files.readString(Paths.get(System.getProperty("user.dir"),
                    "src/main/java/de/hpi/des/hdes/engine/generators/templates/Query.java.template"));
            String uuid = UUID.randomUUID().toString();
            this.query = String.format(query, "Query", "%s");
        } catch (IOException e) {
            System.exit(1);
        }

    }

    public void build(final Topology queryTopology) {
        final List<Node> sortedNodes = queryTopology.getTopologicalOrdering();
        for (final Node node : sortedNodes) {
            if (!this.topology.getNodes().contains(node)) {
                node.accept(this);
            }
        }
        try {
            Files.writeString(Paths.get("final_query.java"), query);
        } catch (IOException e) {
            System.exit(1);
        }
    }

    public void build(final Query query) {
        this.build(query.getTopology());
    }

    @Override
    public <OUT> void visit(SourceNode<OUT> sourceNode) {
        // TODO Auto-generated method stub

    }

    @Override
    public <IN> void visit(SinkNode<IN> sinkNode) {
        // TODO Auto-generated method stub

    }

    @Override
    public <IN, OUT> void visit(UnaryOperationNode<IN, OUT> unaryOperationNode) {
        // TODO Auto-generated method stub

    }

    @Override
    public <IN, OUT> void visit(UnaryGenerationNode<IN, OUT> unaryGenerationNode) {
        // TODO Auto-generated method stub
        String filter = unaryGenerationNode.getOperator().generate();
        query = String.format(query, filter);
    }

    @Override
    public <IN1, IN2, OUT> void visit(BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
        // TODO Auto-generated method stub

    }

}
