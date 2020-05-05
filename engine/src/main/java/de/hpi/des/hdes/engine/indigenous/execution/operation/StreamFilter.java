package de.hpi.des.hdes.engine.indigenous.execution.operation;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Filter;
import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;

/**
 * Operator that filters elements based on a predicate
 *
 * @param <IN> the type of the input elements
 */
public class StreamFilter<IN> extends AbstractTopologyElement<IN>
implements OneInputOperator<IN, IN>, Compilable {

  private final Filter<? super IN> filter;
  
  public StreamFilter(final Filter<? super IN> filter) {
    this.filter = filter;
  }

  @Override
  public native void process(final AData<IN> aData);

  private void collect(final AData<IN> aData) {
    this.collector.collect(aData);
  }

  public String compile() {
      String getValueCode = "jclass cls = env->GetObjectClass(aData);\n"     +
        "jmethodID mGetValue = env->GetMethodID(cls, \"getValue\", \"()Ljava/lang/Object;\");\n" +
        "jobject valueInteger = env->CallObjectMethod(aData, mGetValue);\n";
     
      String getIntValueCode = "jclass integerClass = env->GetObjectClass(valueInteger);\n" +
        "jmethodID getInt = env->GetMethodID(integerClass, \"intValue\", \"()I\");\n" +
        "jint value = env->CallIntMethod(valueInteger, getInt);\n";

      String evalCode = "if(value % 2 == 1){ return; }\n";

      String collectCode = "jclass jObjectClass = env->GetObjectClass(thisObject);\n" +
      "jmethodID collect =  env->GetMethodID(jObjectClass, \"collect\", \"(Lde/hpi/des/hdes/engine/AData;)V\");\n" + 
      "env->CallVoidMethod(thisObject, collect, aData);\n";

      return getValueCode + getIntValueCode + evalCode + collectCode;
  }
}