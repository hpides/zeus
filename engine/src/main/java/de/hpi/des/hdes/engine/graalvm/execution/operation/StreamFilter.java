package de.hpi.des.hdes.engine.graalvm.execution.operation;

import de.hpi.des.hdes.engine.operation.AbstractTopologyElement;
import de.hpi.des.hdes.engine.operation.OneInputOperator;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.udf.Filter;

import java.io.File;
import java.io.IOException;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import java.lang.Integer;

/**
 * Operator that filters elements based on a predicate
 *
 * @param <IN> the type of the input elements
 */
public class StreamFilter<IN> extends AbstractTopologyElement<IN>
    implements OneInputOperator<IN, IN> {

  private final Filter<? super IN> filter;
  private final Context context;
  private final Source source;
  private final Value cpart;

  public StreamFilter(final Filter<? super IN> filter) throws IOException {
    this.filter = filter;
    this.context = Context.newBuilder().allowNativeAccess(true).allowAllAccess(true).build();
    File file = new File("/Users/nils/Documents/MP/hdes_clone/graalvm/StreamFilter.bc");
    this.source = Source.newBuilder("llvm", file).build();
    this.cpart = this.context.eval(this.source);
  }

  @Override
  public void process(final AData<IN> aData) {
    if (aData.getValue() instanceof Integer) {
        Integer val = (Integer) aData.getValue();
        Value result = cpart.getMember("_Z7processi").execute(val.intValue());
        if (result.asBoolean()) {
            this.collector.collect(aData);
        }
    }   
  }
}