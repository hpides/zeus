package de.hpi.des.hdes.engine.generators;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.github.mustachejava.Mustache;

@Slf4j
public class FilterGenerator implements Generatable {

    private final String filter;
    private final PrimitiveType[] types;
    private final StringWriter writer = new StringWriter();

    public FilterGenerator(final PrimitiveType[] types, final String filter) {
        this.filter = filter;
        this.types = types;
    }

    @Getter
    private class TemplateData {
        @Getter
        @AllArgsConstructor
        private class MaterializationData {
            final String varName;
            final int offset;
            final PrimitiveType type;
        }
        final private String condition;
        final private String signature;
        final private String execution;
        private final String application;
        private MaterializationData[] materilization;

        public TemplateData(PrimitiveType[] types, String condition, String execution) {
            String[] functionSplit = condition.replaceAll("[()]", "").split("->");
            if(functionSplit.length != 2) throw new Error("Malformatted function");
            String[] parameters = functionSplit[0].replaceAll(" ", "").split(",");
            if(parameters.length != types.length) throw new Error("Malformatted parameters");
            materilization = new MaterializationData[types.length];
            int offset = 0;
            String cond = functionSplit[1];
            for(int i = 0; i< parameters.length; i++){
                if(!parameters[i].equals("_") && cond.contains(parameters[i])){
                    materilization[i] = new MaterializationData(parameters[i], offset, types[i]);
                    cond = cond.replaceAll(parameters[i], "\\$".concat(parameters[i]));
                    offset = 0;
                } else {
                    offset+=types[i].getLength();
                }
            }
            this.signature = Arrays.stream(materilization).filter(m -> m != null).map(m -> m.getType().getLowercaseName() + " $" +m.getVarName()).collect(Collectors.joining(", "));
            this.application =  Arrays.stream(materilization).filter(m -> m != null).map(m -> m.getVarName()).collect(Collectors.joining(", "));
            this.condition = cond;
            this.execution = execution;
        }
    }

    @Override
    public String generate(String execution) {
        try {
            Mustache template = MustacheFactorySingleton.getInstance().compile("Filter.java.mustache");
            template.execute(writer, new TemplateData(types, filter, execution)).flush();
            return writer.toString();
        } catch (IOException e) {
            log.error(e.toString());
        }
        return "";
    }
}
