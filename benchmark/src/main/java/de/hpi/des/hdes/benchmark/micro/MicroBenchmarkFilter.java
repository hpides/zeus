package de.hpi.des.hdes.benchmark.micro;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.infra.Blackhole;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.operation.Collector;
import lombok.extern.slf4j.Slf4j;

@State(Scope.Benchmark)
public class MicroBenchmarkFilter {

    public de.hpi.des.hdes.engine.operation.StreamFilter<Integer> jvmFilterOperator;
    @Param({ "100000", "500000", "1000000", "5000000"})
    public int iterations;

    @Setup(Level.Invocation)
    public void setUp() throws IOException {
      
      jvmFilterOperator = new de.hpi.des.hdes.engine.operation.StreamFilter<Integer>(e -> e % 2 == 0);
      
      
    }


    @Benchmark
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode({Mode.AverageTime})
    @Timeout(time=20)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void jvmFilterBenchmark(MicroBenchmarkFilter plan, Blackhole bh) {
      plan.jvmFilterOperator.init(new Collector<Integer>(){

        @Override
        public void collect(AData<Integer> t) {
          bh.consume(t);
        }
      });
      for (int i = 1; i < plan.iterations; i++) {
        plan.jvmFilterOperator.sendDownstream(new AData<Integer>(0, 0, false));
      }
    }

}


