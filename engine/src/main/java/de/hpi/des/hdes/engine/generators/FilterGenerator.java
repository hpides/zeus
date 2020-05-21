package de.hpi.des.hdes.engine.generators;

import lombok.Getter;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

public class FilterGenerator<IN> implements Generatable {

    private final String filter;
    private final StringWriter writer = new StringWriter();

    public FilterGenerator(final String filter) {
        this.filter = filter;
    }

    @Getter
    private class TemplateData {
        private String condition;
        private String execution;

        public TemplateData(String condition, String execution) {
            this.condition = condition;
            this.execution = execution;
        }
    }

    @Override
    public String generate(String execution) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("Filter.java.mustache");
            template.execute(writer, new TemplateData(filter, execution)).flush();
            return writer.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return "";
    }
}
