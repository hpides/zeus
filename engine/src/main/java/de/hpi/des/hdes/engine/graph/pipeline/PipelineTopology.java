package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.TempSink;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class PipelineTopology {

    private final List<Pipeline> pipelines;

    public PipelineTopology(final List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }

    public void loadPipelines() {
        pipelines.get(0).loadPipeline(new TempSink(), TempSink.class);
        for (Pipeline pipeline : pipelines.subList(1, pipelines.size())) {
            pipeline.loadPipeline(pipeline.getChild().getPipelineObject(), pipeline.getChild().getPipelineKlass());
        }
    }

    public List<RunnablePipeline> getRunnablePiplines() {
        return this.pipelines.stream().filter(pipeline -> pipeline instanceof RunnablePipeline)
                .map(pipeline -> (RunnablePipeline) pipeline).collect(Collectors.toList());
    }

    public static String getChildProcessMethod(Pipeline parent, Pipeline child) {
        if (child instanceof UnaryPipeline) {
            return "process";
        } else if (child instanceof BinaryPipeline) {
            if (((BinaryPipeline) child).getLeftParent().equals(parent)) {
                return "joinLeftPipeline";
            } else if (((BinaryPipeline) child).getRightParent().equals(parent)) {
                return "joinRightPipeline";
            } else {
                log.error("Unkown parent pipeline in binary pipeline with id: {}", parent.getPipelineId());
                return "";
            }
        } else {
            // log.error("Unknown Pipeline type: {}", child.getClass());
            return "process";
        }
    }
}
