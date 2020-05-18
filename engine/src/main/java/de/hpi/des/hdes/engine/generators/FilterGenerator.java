package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FilterGenerator<IN> implements Generatable {

    private final String filter;

    public FilterGenerator(final String filter) {
        this.filter = filter;
    }

    @Override
    public String generate() {
        try {
            String template = Files.readString(Paths.get(System.getProperty("user.dir"),
                    "src/main/java/de/hpi/des/hdes/engine/generators/templates/Filter.java.template"));
            return String.format(template, filter, "%s");
        } catch (IOException e) {
            System.out.println(e);
        }
        return "";
    }
}
