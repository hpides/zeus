package de.hpi.des.hdes.engine.generators;

import java.io.StringWriter;

public class MapGenerator implements Generatable {

    private final String mapper;
    private final StringWriter writer = new StringWriter();

    public MapGenerator(final String mapper) {
        this.mapper = mapper;
    }

    @Override
    public String generate(String execution) {
        // TODO Auto-generated method stub

        return null;
    }
}