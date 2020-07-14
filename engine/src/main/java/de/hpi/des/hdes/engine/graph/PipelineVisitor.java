package de.hpi.des.hdes.engine.graph;

import java.util.HashMap;
import java.util.UUID;

import de.hpi.des.hdes.engine.generators.templatedata.InterfaceData;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
import de.hpi.des.hdes.engine.graph.pipeline.AJoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.FileSinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.AggregationPipeline;

import lombok.Getter;

@Getter
public abstract class PipelineVisitor {
    private final HashMap<String, InterfaceData> interfaces = new HashMap<>();
    private final HashMap<String, MaterializationData> variables = new HashMap<>();

    public String registerInterface(InterfaceData iface) {
        String id = "c" + UUID.randomUUID().toString();
        interfaces.put(id, iface);
        return id;
    }

    public String registerVariable(MaterializationData var) {
        this.variables.put(var.getVarName(), var);
        return var.getVarName();
    }

    public abstract void visit(UnaryPipeline unaryPipeline);

    public abstract void visit(AggregationPipeline aggregationPipeline);

    public abstract void visit(AJoinPipeline binaryPipeline);

    public abstract void visit(JoinPipeline binaryPipeline);

    public abstract void visit(NetworkSourcePipeline sourcePipeline);

    public abstract void visit(FileSinkPipeline fileSinkPipeline);
}
