package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

import lombok.Getter;

public class JoinGenerator implements BinaryGeneratable {

    private final StringWriter writer = new StringWriter();

    @Getter
    private class TemplateData {
        private String execution;

        public TemplateData(String execution) {
            this.execution = execution;
        }
    }

    @Override
    public String generate(String execution, boolean isLeft) {
        try {
            String file = "JoinRight.java.mustache";
            if (isLeft) {
                file = "JoinLeft.java.mustache";
            }
            Mustache template = MustacheFactorySingleton.getInstance().compile(file);
            template.execute(writer, new TemplateData(execution)).flush();
            return writer.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return "";
    }

}
