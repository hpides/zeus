package de.hpi.des.hdes.engine.graph.pipeline;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.Stoppable;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
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

@Slf4j
@Getter
public abstract class Pipeline {

    @Getter
    protected final List<GenerationNode> nodes;
    protected Class pipelineKlass;
    @Setter
    protected Object pipelineObject;
    private final Set<String> queryIds = new HashSet<>();
    private static URLClassLoader tempClassLoader;
    private final HashMap<String, InterfaceData> interfaces = new HashMap<>();
    protected final HashMap<String, MaterializationData> variables = new HashMap<>();
    protected PrimitiveType[] inputTypes;
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

    protected Pipeline(PrimitiveType[] types, GenerationNode node) {
        this.inputTypes = types;
        for (int i = 0; i < inputTypes.length; i++)
            currentTypes.add(null);
        this.nodes = new ArrayList<>();
        this.nodes.add(node);
    }

    protected Pipeline(PrimitiveType[] types, List<GenerationNode> nodes) {
        this.inputTypes = types;
        for (int i = 0; i < inputTypes.length; i++)
            currentTypes.add(null);
        this.nodes = nodes;
    }

    abstract public String getPipelineId();

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

    protected MaterializationData getVariableAtIndex(int index, String inputName) {
        String varName = currentTypes.get(index);
        if (varName != null) {
            return variables.get(varName);
        }
        int offset = 8;
        for (int i = 0; i < index; i++) {
            offset += inputTypes[i].getLength();
        }
        MaterializationData var = new MaterializationData(variables.size(), offset, inputTypes[index], inputName);
        currentTypes.set(index, var.getVarName());
        variables.put(var.getVarName(), var);
        return var;
    }

    public MaterializationData getVariableAtIndex(int index) {
        return getVariableAtIndex(index, "input");
    }

    protected void removeVariableAtIndex(int index, String inputName) {
        for (int i = index + 1; i < currentTypes.size(); i++) {
            getVariableAtIndex(i, inputName);
        }
        currentTypes.remove(index);
    }

    public void removeVariableAtIndex(int index) {
        removeVariableAtIndex(index, "input");
    }

    protected MaterializationData addVariable(PrimitiveType type, String inputName) {
        MaterializationData var = new MaterializationData(variables.size(), type, inputName);
        currentTypes.add(var.getVarName());
        variables.put(var.getVarName(), var);
        return var;
    }

    public MaterializationData addVariable(PrimitiveType type) {
        return addVariable(type, "input");
    }

    public MaterializationData[] getVariables() {
        return this.variables.values().toArray(new MaterializationData[variables.size()]);
    }

    public String getWriteout(String bufferName) {
        // TODO Optimize Timestamp and watermark copy
        String implementation = "outputBuffer.position(initialOutputOffset+8);\n";
        int copyLength = 0;
        int arrayOffset = 8;
        for (int i = 0; i < currentTypes.size(); i++) {
            if (currentTypes.get(i) == null) {
                copyLength += inputTypes[i].getLength();
            } else {
                MaterializationData var = variables.get(currentTypes.get(i));
                if (copyLength != 0) {
                    implementation = implementation.concat(bufferName).concat(".getBuffer().position(startingPosition+")
                            .concat(Integer.toString(arrayOffset )).concat(");\n").concat(bufferName)
                            .concat(".getBuffer().get(output, initialOutputOffset+")
                            .concat(Integer.toString(arrayOffset)).concat(", ").concat(Integer.toString(copyLength))
                            .concat(");\n");
                    implementation = implementation.concat("outputBuffer.position(initialOutputOffset+")
                            .concat(Integer.toString(arrayOffset + copyLength)).concat(");\n");
                    arrayOffset += copyLength;
                    copyLength = 0;
                }
                implementation = implementation.concat("outputBuffer.put").concat(var.getType().getUppercaseName())
                        .concat("(").concat(var.getVarName()).concat(");\n");
                arrayOffset += var.getType().getLength();
            }
        }
        if (copyLength != 0) {
            implementation = implementation.concat(bufferName).concat(".getBuffer().position(startingPosition+")
                    .concat(Integer.toString(arrayOffset )).concat(");\n").concat(bufferName).concat(".getBuffer().get(output, initialOutputOffset+")
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

    public int getInputTupleLength() {
        int length = 0;
        for (PrimitiveType pt : inputTypes) {
            length += pt.getLength();
        }
        return length;
    }

    public int getOutputTupleLength() {
        int length = 0;
        for (PrimitiveType pt : getOutputTypes()) {
            length += pt.getLength();
        }
        return length;
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
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(ReadBuffer.class, Dispatcher.class)
                    .newInstance(dispatcher.getReadByteBufferForPipeline((UnaryPipeline) this), dispatcher);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    public abstract void replaceParent(Pipeline newParentPipeline);

    public void addQueryId(final String queryId) {
        this.queryIds.add(queryId);
    }

    public void stopQuery(final String queryId, Dispatcher dispatcher) {
        queryIds.remove(queryId);
        if (queryIds.isEmpty()) {

            ((Stoppable) pipelineObject).shutdown();
            // TODO remove files
        }
    }
}
