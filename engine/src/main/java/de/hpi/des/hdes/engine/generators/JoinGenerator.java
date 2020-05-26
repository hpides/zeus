package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.io.StringWriter;

import com.github.mustachejava.Mustache;

import lombok.Getter;

public class JoinGenerator implements BinaryGeneratable {

    private final String keyExtractorLeft;
    private final String keyExtractorRight;
    private final String joinMapper;
    private final StringWriter writer = new StringWriter();

    @Getter
    private class JoinData {
        private final String execution;
        private final String joinKeyExtractor;
        private final String joinMapper;

        public JoinData(String execution, final String joinKeyExtractor, final String joinMapper) {
            this.execution = execution;
            this.joinKeyExtractor = joinKeyExtractor;
            this.joinMapper = joinMapper;
        }
    }

    public JoinGenerator(final String keyExtractorLeft, final String keyExtractorRight, final String joinMapper) {
        super();
        this.keyExtractorLeft = keyExtractorLeft;
        this.keyExtractorRight = keyExtractorRight;
        this.joinMapper = joinMapper;
    }

    @Override
    public String generate(String execution, boolean isLeft) {
        try {
            writer.flush();
            String file = "JoinRight.java.mustache";
            String joinKeyExtractor = this.keyExtractorRight;
            if (isLeft) {
                file = "JoinLeft.java.mustache";
                joinKeyExtractor = this.keyExtractorLeft;
            }
            Mustache template = MustacheFactorySingleton.getInstance().compile(file);
            template.execute(writer, new JoinData(execution, joinKeyExtractor, joinMapper)).flush();
            return writer.toString();
        } catch (IOException e) {
            System.out.println(e);
        }
        return "";
    }

}
