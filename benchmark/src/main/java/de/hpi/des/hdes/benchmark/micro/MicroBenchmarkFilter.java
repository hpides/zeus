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

@State(Scope.Benchmark)
public class MicroBenchmarkFilter {

    @Param({ "100000", "500000", "1000000", "5000000"})
    public int iterations;

    @Setup(Level.Invocation)
    public void setUp() throws IOException {    
    }


    // @Benchmark
    // @Fork(value = 1, warmups = 1)
    // @BenchmarkMode({Mode.AverageTime})
    // @Timeout(time=20)
    // @OutputTimeUnit(TimeUnit.MILLISECONDS)
    // public void Benchmark(MicroBenchmarkFilter plan, Blackhole bh) {
    //  
    // }
}


