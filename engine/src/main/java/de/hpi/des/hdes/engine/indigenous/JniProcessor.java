package de.hpi.des.hdes.engine.indigenous;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.hpi.des.hdes.engine.indigenous.execution.operation.StreamFilter;

public class JniProcessor {    
    static String outputPath = "/tmp/indigenous";
    
    public static void generate(StreamFilter<?> operator) throws IOException {
        String signature = operator.getClass().getName().replace(".", "_");
        String parameters = "jobject aData";
        String template = Files.readString(Paths.get("src/main/resources/stream_filter.cpp.template"));
        String implementation = operator.compile();

        String nativeImplementation = MessageFormat.format(template, signature, parameters, implementation);
        
        String sourceFile = buildOutputPath(operator);
        
        Files.createDirectories(Paths.get(outputPath));

        Files.writeString(Paths.get(sourceFile), nativeImplementation);
    }

    /*
    * compile c header file
    */
    public static void compile(StreamFilter<?> filter) throws IOException, InterruptedException {
        String includePath = System.getProperty("java.home") + "/include";
        String includeLinuxPath = includePath + "/linux";
        String includeIndigenousPath = System.getProperty("user.dir") + "/indigenous";
        String libraryPath = outputPath + "/indigenous.dylib";
        String sourceFile = buildOutputPath(filter);

        String[] command = {
            "g++",
            "-I" + includePath,
            "-I" + includeLinuxPath,
            "-I" + includeIndigenousPath,
            "-o", libraryPath,
            "-shared",
            "-fPIC",
            sourceFile
        };

        Process proc = Runtime.getRuntime().exec(command);

        proc.waitFor();
       
        if(proc.exitValue() == 0)  {
            System.load(libraryPath);
        } else {
            BufferedReader lineReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            lineReader.lines().forEach(System.out::println);
    
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            errorReader.lines().forEach(System.out::println);
        }
    }

    
    /*
    * 
    */
    public static void consume(){
    }

    private static String buildOutputPath(StreamFilter<?> operator) {
        return outputPath + "/" + operator.getClass().getName().replace(".", "_") + ".cpp";
    }
}