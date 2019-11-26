package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import lombok.Value;

@Value
public final class BenchmarkResult {

    private final Generator usedGenerator;
    private final TimedBlockingSource timedSource;
    private final TimedBlockingSink timedSink;

}
