package de.hpi.des.hdes.engine.generators;

import java.io.StringWriter;

public class FlatMapGenerator implements Generatable {

    private final String flatMapper;
    private final StringWriter writer = new StringWriter();

    public FlatMapGenerator(final String flatMapper) {
        this.flatMapper = flatMapper;
    }

    @Override
    public String generate(String execution) {
        // TODO Auto-generated method stub
        return null;
    }
}