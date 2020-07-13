package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.node.JoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import lombok.Getter;

@Getter
public class JoinPipeline extends BinaryPipeline {

    protected JoinPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes, Node binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    public JoinPipeline(Node binaryNode) {
        super(binaryNode);
    }

    public static JoinPipeline of(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes, Node binaryNode) {
        return new JoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public JoinGenerationNode getBinaryNode() {
        return (JoinGenerationNode) this.binaryNode;
    }

    
    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
      // TODO Auto-generated method stub
    }
  
    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
      // TODO Auto-generated method stub
      
    }
}