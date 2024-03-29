package de.hpi.des.hdes.engine.generators;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

import de.hpi.des.hdes.engine.generators.templatedata.FilterData;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;

@Slf4j
public class FilterGenerator implements UnaryGeneratable {

    private final String filter;
    private final PrimitiveType[] types;
    private final StringWriter writer = new StringWriter();

    public FilterGenerator(final PrimitiveType[] types, final String filter) {
        this.filter = filter;
        this.types = types;
    }

    @Override
    public String generate(Pipeline pipeline) {
        try {
            FilterData data = new FilterData(pipeline, types, filter, "free", "input");
            Mustache template = MustacheFactorySingleton.getInstance().compile("Filter.java.mustache");
            template.execute(writer, data).flush();
            return writer.toString();
        } catch (IOException e) {
            log.error(e.toString());
        }
        return "";
    }

    public String generate(Pipeline pipeline, boolean isRight) {
        String inputName = isRight ? "rightInput" : "leftInput";
        String freeFunction = isRight ? "freeRight" : "freeLeft";
        try {
            FilterData data = new FilterData(pipeline, types, filter, isRight, freeFunction, inputName);
            Mustache template = MustacheFactorySingleton.getInstance().compile("Filter.java.mustache");
            template.execute(writer, data).flush();
            return writer.toString();
        } catch (IOException e) {
            log.error(e.toString());
        }
        return "";
    }

    @Override
    public String getOperatorId() {
        String hashBase = filter;
        for (PrimitiveType t : types) {
            hashBase.concat(t.name());
        }
        return hashBase;
    }
}
