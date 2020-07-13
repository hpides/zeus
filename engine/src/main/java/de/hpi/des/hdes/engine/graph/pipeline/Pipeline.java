package de.hpi.des.hdes.engine.graph.pipeline;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.templatedata.InterfaceData;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.DirectoryHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class Pipeline {

    protected Class pipelineKlass;
    @Setter
    protected Object pipelineObject;
    @Setter
    @Getter
    private boolean isLoaded = false;
    private final Set<String> queryIds = new HashSet<>();
    @Getter
    private String initialQueryId;
    private static URLClassLoader tempClassLoader;
    private final HashMap<String, InterfaceData> interfaces = new HashMap<>();
    private final HashMap<String, MaterializationData> variables = new HashMap<>();
    private final PrimitiveType[] inputTypes;
    private final ArrayList<String> currentTypes = new ArrayList<>();
    @Setter
    private Pipeline child;

    public static URLClassLoader getClassLoader() {
        if (tempClassLoader == null) {
            try {
                tempClassLoader = URLClassLoader
                        .newInstance(new URL[] { new File("engine/src/main/java/").toURI().toURL() });
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        return tempClassLoader;
    }

    protected Pipeline(PrimitiveType[] types) {
        this.inputTypes = types;
        for (int i = 0; i < inputTypes.length; i++)
            currentTypes.add(null);
    }

    public String getPipelineId() {
        return "c".concat(Integer.toString(hashCode()));
    }

    @Override
    public boolean equals(final Object p) {
        if (p instanceof Pipeline) {
            return ((Pipeline) p).getPipelineId().equals(getPipelineId());
        }
        return false;
    }

    public InterfaceData registerInterface(String returnType, PrimitiveType[] types) {
        String id = "c" + UUID.randomUUID().toString().replace("-", "");
        InterfaceData iface = new InterfaceData(id, returnType, types);
        interfaces.put(id, iface);
        return iface;
    }

    public InterfaceData[] getInterfaces() {
        return interfaces.values().toArray(new InterfaceData[interfaces.size()]);
    }

    public MaterializationData getVariableAtIndex(int index) {
        String varName = currentTypes.get(index);
        if (varName != null) {
            return variables.get(varName);
        }
        int offset = 0;
        for (int i = 0; i < index; i++) {
            offset += inputTypes[i].getLength();
        }
        MaterializationData var = new MaterializationData(variables.size(), offset, inputTypes[index]);
        currentTypes.set(index, var.getVarName());
        variables.put(var.getVarName(), var);
        return var;
    }

    public void removeVariableAtIndex(int index) {
        for (int i = index + 1; i < currentTypes.size(); i++) {
            getVariableAtIndex(i);
        }
        currentTypes.remove(index);
    }

    public MaterializationData[] getVariables() {
        return this.variables.values().toArray(new MaterializationData[variables.size()]);
    }

    public MaterializationData addVariable(PrimitiveType type) {
        MaterializationData var = new MaterializationData(variables.size(), type);
        currentTypes.add(var.getVarName());
        variables.put(var.getVarName(), var);
        return var;
    }

    public String getWriteout(String bufferName) {
        // TODO Account for copy of watermark and timestamp
        String implementation = "";
        int copyLength = 0;
        int arrayOffset = 0;
        for (int i = 0; i < currentTypes.size(); i++) {
            if (currentTypes.get(i) == null) {
                copyLength += inputTypes[i].getLength();
            } else {
                MaterializationData var = variables.get(currentTypes.get(i));
                if (copyLength != 0) {
                    implementation = implementation.concat(bufferName).concat(".get(output, ")
                            .concat(Integer.toString(arrayOffset)).concat(", ").concat(Integer.toString(copyLength))
                            .concat(");\n").concat("outputBuffer.position(outputBuffer.position() + ")
                            .concat(Integer.toString(copyLength)).concat(");\n");
                    copyLength = 0;
                } else {
                    implementation = implementation.concat("outputBuffer.put").concat(var.getType().getUppercaseName())
                            .concat("(").concat(var.getVarName()).concat(");\n");
                }
                arrayOffset += var.getType().getLength();
            }
        }
        if (copyLength != 0) {
            implementation = implementation.concat(bufferName).concat(".get(output, ")
                    .concat(Integer.toString(arrayOffset)).concat(", ")
                    .concat(Integer.toString(copyLength).concat(");\n"));
            copyLength = 0;
        }
        return implementation;
    }

    public PrimitiveType[] getOutputTypes() {
        PrimitiveType[] types = new PrimitiveType[currentTypes.size()];
        for (int i = 0; i < currentTypes.size(); i++) {
            if (currentTypes.get(i) == null) {
                types[i] = inputTypes[i];
            } else {
                types[i] = variables.get(currentTypes.get(i)).getType();
            }
        }
        return types;
    }

    public abstract void accept(PipelineVisitor visitor);

    public abstract void addParent(Pipeline pipeline, GenerationNode childNode);

    public abstract void addOperator(GenerationNode operator, GenerationNode childNode);

    protected String getFilePath() {
        return DirectoryHelper.getTempDirectoryPath() + getPipelineId() + ".java";
    }

    protected void compileClass() {
        Path javaFile = Paths.get(this.getFilePath());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        List<String> optionList = new ArrayList<String>();
        if (this.getChild() != null) {
            optionList.addAll(Arrays.asList("-classpath", DirectoryHelper.getClassPathWithTempPackage()));
        }

        compiler.getTask(null, null, null, optionList, null, compiler.getStandardFileManager(null, null, null)
                .getJavaFileObjects(javaFile.toFile().getAbsolutePath())).call();
        try {
            pipelineKlass = Class.forName("de.hpi.des.hdes.engine.temp." + this.getPipelineId(), true,
                    Pipeline.getClassLoader());
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        this.setLoaded(true);
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(childKlass)
                    .newInstance(dispatcher.getReadByteBufferForPipeline((UnaryPipeline) this), dispatcher);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    public abstract void replaceParent(Pipeline newParentPipeline);

    public void addQueryId(String queryId) {
        this.queryIds.add(queryId);   
	}

    public void setInitialQueryId(String queryId) {
        this.queryIds.add(queryId);
        this.initialQueryId = queryId;
	}
}
