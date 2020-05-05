package de.hpi.des.hdes.engine.graalvm;

import java.io.File;
import java.io.IOException;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public class GraalVMProcessor {
    static void process() throws IOException {

        Context context = Context.newBuilder().allowNativeAccess(true).build();
        File file = new File(System.getProperty("user.dir") + "/../graalvm/StreamFilter.bc");
        Source source = Source.newBuilder("llvm", file).build();
        Value cpart = context.eval(source);
        Value result = cpart.getMember("mul_add").execute(2, 4, 3);
        System.out.println("Result: " + result.asInt());
    }
}
