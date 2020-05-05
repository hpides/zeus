package de.hpi.des.hdes.engine.indigenous.execution.operation;

/**
 * An compilable can be used to generate cpp code for an operator.
 **/ 
public interface Compilable {
    
    String compile();
}