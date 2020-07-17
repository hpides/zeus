package de.hpi.des.hdes.engine.generators;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.github.mustachejava.Mustache;

import de.hpi.des.hdes.engine.generators.templatedata.InterfaceData;
import de.hpi.des.hdes.engine.generators.templatedata.MapData;
import de.hpi.des.hdes.engine.generators.templatedata.MapDataList;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.udf.LambdaString;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MapGenerator implements Generatable {
    private final Tuple mapper;
    private final StringWriter writer = new StringWriter();

    public MapGenerator(final Tuple mapper) {
        this.mapper = mapper;
    }

    @Override
    public String generate(Pipeline pipeline) {
        try {
            List<MapData> data = new ArrayList<>();
            Tuple t = this.mapper.getFirst();
            if (!this.mapper.isLast()) {
                log.warn("The mapper given may not be complete.");
            }
            while (!t.isLast()) {
                switch (t.getOperation()) {
                    case GET: {
                        for (int i = 0; i < t.getTypes().length - 1; i++) {
                            if (i != t.getIndex()) {
                                pipeline.removeVariableAtIndex(i);
                            }
                        }
                        break;
                    }
                    case REMOVE: {
                        pipeline.removeVariableAtIndex(t.getIndex());
                        break;
                    }
                    case ADD: {
                        MaterializationData d = pipeline.addVariable(t.getType());
                        LambdaString l = LambdaString.analyze(t.getTypes(), t.getTransformation());
                        InterfaceData interfaceName = pipeline.registerInterface(t.getType().getLowercaseName(),
                                l.getInterfaceDef());
                        String application = Arrays.stream(l.getMaterializationData())
                                .mapToObj(i -> pipeline.getVariableAtIndex(i).getVarName())
                                .collect(Collectors.joining(", "));
                        data.add(new MapData(d.getVarName(), l.getSignature(), l.getExecution(), application,
                                interfaceName.getInterfaceName()));
                        break;
                    }
                    case MUTATE: {
                        MaterializationData d = pipeline.getVariableAtIndex(t.getIndex());
                        LambdaString l = LambdaString.analyze(t.getTypes(), t.getTransformation());
                        InterfaceData interfaceName = pipeline.registerInterface(t.getType().getLowercaseName(),
                                l.getInterfaceDef());
                        String application = Arrays.stream(l.getMaterializationData())
                                .mapToObj(i -> pipeline.getVariableAtIndex(i).getVarName())
                                .collect(Collectors.joining(", "));
                        data.add(new MapData(d.getVarName(), l.getSignature(), l.getExecution(), application,
                                interfaceName.getInterfaceName()));
                        break;
                    }
                }
                t = t.getNextTuple();
            }
            Mustache template = MustacheFactorySingleton.getInstance().compile("Map.java.mustache");
            template.execute(writer, new MapDataList(data)).flush();
            return writer.toString();
        } catch (IOException e) {
            log.error(e.toString());
        }
        return "";
    }

    @Override
    public String getOperatorId() {
        return mapper.getId();
    }
}
