package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

import lombok.Getter;

public class AggregateGenerator implements Generatable {

    private final StringWriter writer = new StringWriter();

    @Getter
    private class AggregationData {
    }

    @Override
    public String generate(String execution) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("Aggregation.java.mustache");
            template.execute(writer, new AggregationData()).flush();
            return writer.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return "";
    }

}